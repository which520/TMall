package com.which.realTime

import com.which.TMall.common.bean.AdsInfo
import com.which.TMall.common.util.MyKafkaUtil
import com.which.realTime.app.{AreaCityAdsPerDay, BlackListApp, DayAreaAdsTop3, LastHourAdsClickApp}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","which101")
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
    checkedAdsInfo.repartition(4)
    //BlackListApp.checkUserToBlackList(checkedAdsInfo)
    //val value = AreaCityAdsPerDay.areaCityAdsCountPerDay(checkedAdsInfo,sc)
    //DayAreaAdsTop3.getDayAreaAdsTop3(value)
    LastHourAdsClickApp.getLastHourClickApp(checkedAdsInfo,sc)
    ssc.start()
    ssc.awaitTermination()
  }
}
