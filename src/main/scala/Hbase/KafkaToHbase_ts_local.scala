package Hbase

import java.util

import DateUtil.DateTansTimestamp
import Hbase.Utils.Json.ParseJsonFormat
import Hbase.Utils.{Conn, CreateStream, GetProperties, OpDb}
import MD5.Md5Hash
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object KafkaToHbase_ts_local {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("K2SS2H_os_ts").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    //设置反压，限制读取
//    conf.set("spark.streaming.backpressure.enabled","true")
//    conf.set("spark.streaming.kafka.maxRatePerPartition","150")
    val sc = new SparkContext(conf)
    val pro = new GetProperties()
    val ssc = new StreamingContext(sc, Seconds(pro.space_ts))
    //构建topic的array
    val array = Array(pro.topic_1_ts)
    //构建stream
    val stream = new CreateStream().createStreamMethod(ssc, pro.groupid_ts, array, pro.mysqlTableName_ts)

    stream.foreachRDD({
      rdd =>
        //获取偏移量范围
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //将生成的人rdd写入到hbase中
        //定义时间戳转换
        val dataTrans = new DateTansTimestamp()
        //定义操作数据库对象
        val opdb = new OpDb()
        //定义Json解析对象
        val parseJson = new ParseJsonFormat()
        //过滤出ts的数据
        val rdd_ts = rdd.filter(str => {
          val row = str.value()
          parseJson.parseJson1(row, "type").equalsIgnoreCase("ts")
        })
        //ts数据写入Hbase
        rdd_ts.foreachPartition(partirionser => {
          val conn_ts = new Conn().getConn()
          val puts_ts = new util.ArrayList[Put]
          val tableName_ts = TableName.valueOf(pro.hbaseTableName_ts)
          val table_ts = conn_ts.getTable(tableName_ts)
          try {
            partirionser.foreach(str => {
              val row = str.value()
              val dataCode = parseJson.parseJson4(row, "data", "content", "msg", "dataCode")
              val businessSysId = parseJson.parseJson2(row, "data", "businessSysId")
              val instructionType = parseJson.parseJson4(row, "data", "content", "msg", "instructionType")
              var deviceTime = parseJson.parseJson4(row, "data", "content", "msg", "deviceTime")
              val serviceTime = parseJson.parseJson4(row, "data", "content", "msg", "serviceTime")
              if (deviceTime.isEmpty || deviceTime.equals("null")) {
                deviceTime = serviceTime
              }
              val number  = (deviceTime.substring(0, 10).hashCode)%30
//              println(number)
              val partition = Partition.PartitonHbase.partitionHbase(number, number.toString.length)
              val rowkey = partition + Md5Hash.md5HashMethod(businessSysId + instructionType) + dataTrans.tranTimeToLong(deviceTime) + Md5Hash.md5HashMethod(dataCode)
//              println(rowkey)
              //写入hbase
              //将所有列加入列簇cf中
              val put = new Put(Bytes.toBytes(rowkey))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("data_json_ts"), Bytes.toBytes(parseJson.parseJson1(row, "data")))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dataName"), Bytes.toBytes(parseJson.parseJson4(row, "data", "content", "msg", "dataName")))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("deviceType"), Bytes.toBytes(parseJson.parseJson4(row, "data", "content", "msg", "deviceType")))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("deviceID"), Bytes.toBytes(parseJson.parseJson4(row, "data", "content", "msg", "deviceID")))
              puts_ts.add(put)
              if (puts_ts.size() % 30000 == 0) {
                table_ts.put(puts_ts)
                puts_ts.clear()
              }
            })
          } catch {
            case e: Exception => e.printStackTrace
          } finally {
            table_ts.put(puts_ts)
            table_ts.close()
            conn_ts.close()
          }
        })
        //偏移量存入mysql，使用scalikejdbc框架事务
        try {
          opdb.replaceDB(offsetRanges, pro.mysqlTableName_ts, pro.groupid_ts)
        } catch {
          case e: Exception => e.printStackTrace
        }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
