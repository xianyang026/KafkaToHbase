package Hbase.Utils



import java.util

import Hbase.Utils.Json.ParseJsonFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NewTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("K2SS2H_ly2").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //设置反压，限制读取
    //conf.set("spark.streaming.backpressure.enabled","true")
    //conf.set("spark.streaming.kafka.maxRatePerPartition","10000")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(60))
    //构建topic的array
    val array = Array("d2Collection")
    //构建stream
    // val stream: InputDStream[ConsumerRecord[String, String]]
    val stream = new CreateStream().createStreamMethod(ssc, "test_updata3", array,"d2collection_hbase")
    stream.foreachRDD({
      rdd=>
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //用来处理kafka的rdd

        val opdb = new OpDb()
        val parseJson = new ParseJsonFormat()//定义Json解析对象

        //过滤出dataCode等于deciveMsg的数据
        val rdd_ts = rdd.filter(str => {
          val row = str.value()
          val lines = row.split("\\|")
          val dataCode=lines(5)//字段名称
          dataCode.equalsIgnoreCase("deviceMsg")
        })

        //处理60s的rdd
        val rdd1= rdd_ts.mapPartitions(iter => {
          iter.map(t => {
            //取到kafka的value
            val row = t.value()
            //数据格式
            //f4b9df097c|shadow|1068|15345|collection|sn|28|2020-04-28 11:00:04|2020-04-28 13:23:53|1542|1104942|null|null|4587025087473
            val lines = row.split("\\|")

            val rowkey = lines(13)//设备唯一标识
            var dTime = lines(7)
            var sTime = lines(8)
            var jingdu = lines(11)
            var weidu = lines(12)

            val dType=lines(2)//设备类型
            val routeDevSysIdStr=lines(14)//路由器唯一标识
            val roteType=lines(9)//路由器类型
            val value=lines(6)//value
              //硬件版本,软件版本,下发方式,协议版本会在value里提现
              val softVersion=parseJson.parseJson1(value,"softVersion")//软件版本
              val hardwareVersion=parseJson.parseJson1(value,"hardwareVersion")//硬件版本
              val deliveryMethod=parseJson.parseJson1(value,"deliveryMethod")//下发方式
              val version=parseJson.parseJson1(value,"version")//协议版本
             println("111111111"+rowkey+","+dTime+","+sTime+","+jingdu+","+weidu+","+
             dType+","+routeDevSysIdStr+","+roteType,","+softVersion+","+hardwareVersion+","+deliveryMethod+","+
             version)
            caseHbase(rowkey, dTime, sTime, jingdu, weidu,dType,routeDevSysIdStr,roteType,softVersion,hardwareVersion,deliveryMethod,version)
          })
        })


        //hbase 配置
        val conf1 = HBaseConfiguration.create()

        //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
        conf1.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
        //设置zookeeper连接端口，默认2181
        conf1.set("hbase.zookeeper.property.clientPort", "2181")
        conf1.set(TableInputFormat.INPUT_TABLE, "test_updata3")
        //创建一个hbase连接对象
        //        val conn_ts  = ConnectionFactory.createConnection(conf1)
        //        val tableName_ts = TableName.valueOf("test_updata2") // habase的表名
        //      val table_ts = conn_ts.getTable(tableName_ts)
        //表属性描述对象
        val scan = new Scan()
        //把需要扫描的列簇进去
        scan.addFamily(Bytes.toBytes("info"))
        conf1.set(TableInputFormat.SCAN, convertScanToString(scan))

        //准备构建一个hbaseRDD
        //实现hbase批量读取 hbaseRDD
        val hrdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf1,
          classOf[TableInputFormat],
          classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
          classOf[org.apache.hadoop.hbase.client.Result])


        //hbase rdd
        val rdd2: RDD[caseHbase] = hrdd.mapPartitions(iter => {
          iter.map(t => {
            val value = t._2
            val rowkey = Bytes.toString(value.getRow)
            val weidu = Bytes.toString(value.getValue("info".getBytes, "weidu".getBytes))
            val dTime = Bytes.toString(value.getValue("info".getBytes, "dTime".getBytes))
            val sTime = Bytes.toString(value.getValue("info".getBytes, "sTime".getBytes))
            val jingdu = Bytes.toString(value.getValue("info".getBytes, "jingdu".getBytes))
            val dType = Bytes.toString(value.getValue("info".getBytes, "dType".getBytes))
            val routeDevSysIdStr = Bytes.toString(value.getValue("info".getBytes, "routeDevSysIdStr".getBytes))
            val roteType = Bytes.toString(value.getValue("info".getBytes, "roteType".getBytes))
            val softVersion = Bytes.toString(value.getValue("info".getBytes, "softVersion".getBytes))
            val hardwareVersion = Bytes.toString(value.getValue("info".getBytes, "hardwareVersion".getBytes))
            val deliveryMethod = Bytes.toString(value.getValue("info".getBytes, "deliveryMethod".getBytes))
            val version = Bytes.toString(value.getValue("info".getBytes, "version".getBytes))
            //println("22222222222" + rowkey + "," + dTime + "," + sTime + "," + jingdu + "," + weidu)
            caseHbase(rowkey, dTime, sTime, jingdu, weidu,dType,routeDevSysIdStr,roteType,softVersion,hardwareVersion,deliveryMethod,version)
          })
        })


        val rdd3 = rdd1.union(rdd2)


        val resultRDD = rdd3.groupBy(_.rowkey).map(t => {
          var rowkey = "00"
          var maxTime = "00"
          var sTime = "00"
          var jingdu = "00"
          var weidu = "00"
          var dType = "00"
          var routeDevSysIdStr = "00"
          var roteType = "00"
          var softVersion = "00"
          var hardwareVersion = "00"
          var deliveryMethod = "00"
          var version = "00"

          val iterator: Iterator[caseHbase] = t._2.toIterator

          val hbases: List[caseHbase] = iterator.toList.sortBy(_.dTime)

          for(se<- hbases){
            val dTime=se.dTime
            if(dTime >= maxTime){
              rowkey=se.rowkey
              maxTime=se.dTime
              sTime=se.sTime
              jingdu=se.jingdu
              weidu=se.weidu
              dType=se.dType
              routeDevSysIdStr=se.routeDevSysIdStr
              roteType=se.roteType
              softVersion=se.softVersion
              hardwareVersion=se.hardwareVersion
              deliveryMethod=se.deliveryMethod
              version=se.version
            }
          }
          caseHbase(rowkey, maxTime, sTime, jingdu, weidu,dType,routeDevSysIdStr,roteType,softVersion,hardwareVersion,deliveryMethod,version)

        })




        //可以向hbase中写数据了
        resultRDD.foreachPartition(partition => {
          val conn_ts = new Conn().getConn()
          val puts_ts = new util.ArrayList[Put] //封装用
          val tableName_ts = TableName.valueOf("test_updata3") // habase的表名
          val table_ts = conn_ts.getTable(tableName_ts) //表属性描述对象
          try {
            partition.foreach(q => {

              val rowkey: String = q.rowkey
              val dtime = q.dTime
              val stime = q.sTime
              val jd = q.jingdu
              val wd = q.weidu
              val dType = q.dType
              val routeDevSysIdStr = q.routeDevSysIdStr
              val roteType = q.roteType
              val softVersion = q.softVersion
              val hardwareVersion = q.hardwareVersion
              val deliveryMethod = q.deliveryMethod
              val version = q.version
              // println("rowkey:"+rowkey+",dtime:"+dtime+",stime:"+stime+",jingdu:"+jd+",weidu:"+wd)
              val put = new Put(Bytes.toBytes(rowkey))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dTime"), Bytes.toBytes(dtime))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sTime"), Bytes.toBytes(stime))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("jingdu"), Bytes.toBytes(jd))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(wd))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(dType))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(routeDevSysIdStr))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(roteType))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(softVersion))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(hardwareVersion))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(deliveryMethod))
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weidu"), Bytes.toBytes(version))
              puts_ts.add(put)
              if (puts_ts.size() % 60000 == 0) {
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
          opdb.replaceDB(offsetRanges, "d2collection_hbase", "test_updata3")
        } catch {
          case e: Exception => e.printStackTrace
        }




    })


  }


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


  case class caseHbase (rowkey:String,dTime:String,sTime:String,jingdu:String,weidu:String,dType:String,routeDevSysIdStr:String,roteType:String,
                        softVersion:String,hardwareVersion:String,deliveryMethod:String,version:String)

}
