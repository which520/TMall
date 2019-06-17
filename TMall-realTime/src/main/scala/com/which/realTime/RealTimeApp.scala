package com.which.realTime

import com.which.TMall.common.bean.AdsInfo
import com.which.TMall.common.util.MyKafkaUtil
import com.which.realTime.app.BlackListApp
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RealTimeApp")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(1))
    val recordStream = MyKafkaUtil.getDstream(ssc,"ads_log")
    val adsInfoDstream = recordStream.map {
      record =>
        val split = record.value().split(",")
        AdsInfo(split(0).toLong, split(1), split(2), split(3).toLong, split(4).toLong)
    }
    val checkedAdsInfo = BlackListApp.checkUserFromBlackList(adsInfoDstream,sc)
    BlackListApp.checkUserToBlackList(checkedAdsInfo)
    checkedAdsInfo.print
    ssc.start()
    ssc.awaitTermination()
  }
}
