package com.which.tmall.acc

import org.apache.spark.util.AccumulatorV2

/**
  * 自定义累加器来实现只遍历一次对cid的click，order,pay三个维度的数量的统计
  */
class CidTop10Acc extends AccumulatorV2[(String,String),Map[(String,String),Long]]{
  var map = Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, String), Map[(String, String), Long]] = {
    val acc = new CidTop10Acc
    acc.map ++= map
    acc
  }

  override def reset(): Unit = {
    map = Map[(String,String),Long]()
  }

  override def add(v: (String, String)): Unit = {
    map += v -> (map.getOrElse(v,0L) + 1L)
  }

  override def merge(other: AccumulatorV2[(String, String), Map[(String, String), Long]]): Unit = {
    other.asInstanceOf[CidTop10Acc].map.foreach{
      v => map += v._1 -> (map.getOrElse(v._1,0L) + v._2)
    }
  }

  override def value: Map[(String, String), Long] = map
}
