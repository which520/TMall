package com.which.TMall.common.util

import java.{io, lang}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {
  val config = ConfigurationUtil("config.properties")
  val broker_list: String = config.getString("kafka.broker.list")
  private val kafkaParams = Map(
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "commerce-consumer-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true:java.lang.Boolean)
  )
  def getDstream(ssc:StreamingContext,topic:String)={
    KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParams)
    )
  }
}
