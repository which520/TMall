package com.which.realTime.app

import com.which.TMall.common.bean.AdsInfo
import com.which.TMall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

object AreaCityAdsPerDay {
  def areaCityAdsCountPerDay(filterAdsInfoDstream:DStream[AdsInfo],sc: SparkContext) ={
    sc.setCheckpointDir("hdfs://hadoopSlaver-202:9000/checkpoint2")
    val resultDstream = filterAdsInfoDstream.map(info => {
      (s"${info.dayStrign}:${info.area}:${info.city}:${info.adid}", 1L)
    }).reduceByKey(_ + _).updateStateByKey((seq:Seq[Long],opt:Option[Long])=>{Some(seq.sum + opt.getOrElse(0L))
    })
    resultDstream.foreachRDD(rdd =>{
      rdd.foreachPartition(it => {
        val client = RedisUtil.getJedisClient
        it.foreach({
          case(fieldString,count)=>client.hset("day:area:city:adsCount",fieldString,count.toString)
        })
        client.close()
      })
    })
    resultDstream
  }
}
