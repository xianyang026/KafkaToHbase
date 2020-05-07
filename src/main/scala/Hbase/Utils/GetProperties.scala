package Hbase.Utils

import java.io.FileInputStream
import java.util.Properties

class GetProperties extends Serializable {
  val properties = new Properties()
  //文件要放到resource文件夹下
//val path = Thread.currentThread().getContextClassLoader.getResource("ts.properties").getPath
  properties.load(new FileInputStream("/home/liuyang/data/spark_properties/ts.properties"))
//properties.load(new FileInputStream(path))
  //读取键为brokerList的数据的值,kafka集群地址
  val brokerList = properties.getProperty("brokerList")
  //读取键为topic_1_k1的数据的值
  val topic_1_k1 = properties.getProperty("topic_1_k1")

  //读取键为topic_2_k1的数据的值
  val space_ts = properties.getProperty("space_ts").toLong

  //读取键为topic_2_k1的数据的值
  val topic_1_ts = properties.getProperty("topic_1_ts")

  //读取键为groupid_ts的数据的值
  val groupid_ts = properties.getProperty("groupid_ts")


  val mysqlTableName_ts = properties.getProperty("mysqlTableName_ts")

  val hbaseTableName_ts = properties.getProperty("hbaseTableName_ts")


  val zookeeper_list = properties.getProperty("zookeeper_list")




  //读取键为topic_2_k1的数据的值
  val topic_2_k1 = properties.getProperty("topic_2_k1")
}
