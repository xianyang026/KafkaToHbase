package Hbase.Utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

class Conn {

  def getConn() = {
    val conf = HBaseConfiguration.create()

    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection  = ConnectionFactory.createConnection(conf)
    connection
  }
}
