package Hbase

import DateUtil.DateTansTimestamp
import MD5.Md5Hash
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

object ReadHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkOpHbase_read").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    val tablename = "ts_data_new"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "tdr01,tdr02,tdr03")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    //定义时间戳转换
    val dataTrans = new DateTansTimestamp();
    //添加scan
    val scan = new Scan()
//    val number  = (deviceTime.substring(0, 10).hashCode)%30
//    println(number)
//    val partition = Partition.PartitonHbase.partitionHbase(number, number.toString.length)
//    val rowkey = partition + Md5Hash.md5HashMethod(businessSysId + instructionType) + dataTrans.tranTimeToLong(deviceTime) + Md5Hash.md5HashMethod(dataCode)
    //              println(rowkey)
  val number  = ("2020-04-01".hashCode)%30
    val partition = Partition.PartitonHbase.partitionHbase(number, number.toString.length)
    var rowkey = partition + Md5Hash.md5HashMethod(33 + "collection") + dataTrans.tranTimeToLong("2020-04-01 14:01:23")
    println(rowkey)
    var rowkey2 =  partition + Md5Hash.md5HashMethod(33 + "collection") + dataTrans.tranTimeToLong("2020-04-01 14:02:23")
    println(rowkey2)
    println(Bytes.toBytes(rowkey).toString)
    println(new String(Bytes.toBytes(rowkey)).toString)
    //扫描的表rowkey的开始和结束
    scan.setStartRow(rowkey.getBytes)
        scan.setStopRow(rowkey2.getBytes)
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))


    val hrdd = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultrdd = hrdd.repartition(2)
    //打印结果
    resultrdd.foreach{case(_,value)=>{
      val key = Bytes.toString(value.getRow)
      val name = Bytes.toString(value.getValue("data".getBytes, "dataName".getBytes))
      //      val url = Bytes.toString(value.getValue("cf".getBytes, "url".getBytes))
      //      val age = Bytes.toString(value.getValue("cf".getBytes, "sn".getBytes))
      println("rowkey:"+key+" "+"dataName:"+name)
    }
    }

    sc.stop()

  }
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)

  }
}
