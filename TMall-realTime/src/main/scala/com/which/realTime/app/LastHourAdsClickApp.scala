package com.which.realTime.app

import java.text.SimpleDateFormat

import com.which.TMall.common.bean.AdsInfo
import com.which.TMall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

object LastHourAdsClickApp {
  def getLastHourClickApp(filterDstream:DStream[AdsInfo],sc: SparkContext) ={
    sc.setCheckpointDir("hdfs://hadoopMaster-201:9000/checkpoint2")
    //filterDstream.cache()
    val windowStreamAdsInfo = filterDstream.window(Minutes(60),Seconds(20))
    val result1 = windowStreamAdsInfo.map(info => {
      val dateFormat = new SimpleDateFormat("HH:mm")
      ((info.adid, dateFormat.format(info.timestamp)), 1)
    }).reduceByKey(_ + _)
      .map(x => {
        (x._1._1, (x._1._2,x._2))
      }).groupByKey().map(x =>(x._1,x._2.toList))

    result1.foreachRDD(rdd => {
      rdd.foreachPartition(it =>{
        val client = RedisUtil.getJedisClient
        it.foreach({
            case(adid,it2)=>{
            import org.json4s.JsonDSL._
            client.hset("dd",adid.toString,JsonMethods.compact(JsonMethods.render(it2.sortBy(_._1))))
          }})
        client.close()
      })
    })
  }
}
