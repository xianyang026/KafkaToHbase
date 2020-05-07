package Hbase.Utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scalikejdbc.{DB, SQL}

class OpDb {
  //查询偏移量
  def selectDB(table:String,groupid:String) = {
    val fromdbOffset: Map[TopicPartition, Long] =
      DB.readOnly { implicit session =>
        SQL(s"select * from `${table}` where groupId = '${groupid}'")
          .map(rs => (new TopicPartition(rs.string("topic"), rs.int("partitions")), rs.long("untilOffset")))
          .list().apply()
      }.toMap
    fromdbOffset
  }

  //查询偏移量
  def replaceDB(offsetRanges: Array[OffsetRange],table:String,groupid:String) = {
    DB.localTx { implicit session =>
      for (or <- offsetRanges) {
        SQL(s"replace into `${table}`(groupId,topic,partitions,untilOffset) values(?,?,?,?)")
          .bind(groupid, or.topic, or.partition, or.untilOffset).update().apply()
      }
    }
  }



}
