package Hbase.Utils

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import scalikejdbc.config.DBs


class CreateStream {
  def createStreamMethod(ssc:StreamingContext,groupid:String,array:Array[String],mysqlTableName:String) = {

    //配置kafka连接规则
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "tdr01:9092,tdr02:9092,tdr03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //指向主题
    val topics = array

    //加载配置
    DBs.setup()
    //查询mysql中存储的偏移量
    val fromdbOffset: Map[TopicPartition, Long] = new OpDb().selectDB(mysqlTableName,groupid)
    //程序启动，拉取kafka的消息。
    val stream = if (fromdbOffset.size == 0) {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    } else {
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromdbOffset.keys.toList, kafkaParams, fromdbOffset)
      )
    }
    stream
  }
}
