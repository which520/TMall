package com.which.realTime.app

import com.which.TMall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods


// "date:area:city:ads" -> field:   2018-11-26:华北:北京:5     value: 12001
//area:ads:top3:2019-03-23  华南                {广告1: 1000, 广告2: 500}
object DayAreaAdsTop3 {
  def getDayAreaAdsTop3(areaCityAdsPerDay: DStream[(String,Long)])={
    val result1 = areaCityAdsPerDay.map({
      case (key, count) => {
        val split = key.split(":")
        (s"area:ads:top3:${split(0)}:${split(1)}", (s"${split(3)}", count))
      }
    }).groupByKey()
      .map({case(key,it)=>{
        import org.json4s.JsonDSL._
        (key.split(":").slice(0,3).mkString(":"),Map((key.split(":")(3),JsonMethods.compact(JsonMethods.render(it.toList.sortBy(_._2).take(3))))))
    }})
    result1.foreachRDD(rdd => {
      rdd.foreachPartition(it =>{
        val client = RedisUtil.getJedisClient
        import scala.collection.JavaConversions._
        it.foreach({
          case(key,map)=>{
            client.hmset(key,map)
        }})
        client.close()
      })
    })
  }
}
