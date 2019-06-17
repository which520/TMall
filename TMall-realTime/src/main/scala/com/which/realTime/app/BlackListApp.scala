package com.which.realTime.app

import com.which.TMall.common.bean.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListApp {
  val redisIp = "hadoopMaster-201"
  val redisPort = 6379
  val countKey = "user:day:adsclick"
  val blacklistKey = "blacklist"

  def checkUserFromBlackList(adsClickInfoDStream:DStream[AdsInfo],sc:SparkContext)={
    adsClickInfoDStream.transform(rdd=>{
      val jedis = new Jedis(redisIp,redisPort)
      val blackList = jedis.smembers(blacklistKey)
      val blackListBD = sc.broadcast(blackList)
      jedis.close()
      rdd.filter(adsInfo => {
        !blackListBD.value.contains(adsInfo.userid.toString)
      })})
  }
  def checkUserToBlackList(adsInfoDStream:DStream[AdsInfo]): Unit ={
    adsInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(adsInfoIt => {
        val jedis = new Jedis(redisIp,redisPort)
        adsInfoIt.foreach(adsInfo => {
          val countField = s"${adsInfo.userid}:${adsInfo.dayStrign}:${adsInfo.adid}"
          jedis.hincrBy(countKey,countField,1)
          if(jedis.hget(countKey,countField).toLong >= 100){jedis.sadd(blacklistKey,adsInfo.userid.toString)}
        })
        jedis.close()
      })
    })
  }

}
