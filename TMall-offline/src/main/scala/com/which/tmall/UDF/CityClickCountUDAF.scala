package com.which.tmall.UDF

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

class CityClickCountUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(StructField("city_name",StringType)::Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("city_count",MapType(StringType,LongType))::StructField("total_click",LongType)::Nil)
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val city_name = input.getString(0)
    val map = buffer.getAs[Map[String,Long]](0)
    buffer(0) = map + (city_name->(map.getOrElse(city_name,0L) + 1L))
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getAs[Map[String,Long]](0)
    val map2 = buffer2.getAs[Map[String,Long]](0)
     buffer1(0) = map1.foldLeft(map2){
      case (map,(city_name,count)) => {map + (city_name -> (map.getOrElse(city_name,0L) + count))}
    }
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    val map = buffer.getAs[Map[String,Long]](0)
    val total_count = buffer.getLong(1)
        val formater = new DecimalFormat("0.00%")
    val top2City_click = map.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)
    val click_ration = top2City_click.map(x => (x._1,x._2.toDouble/total_count))
    if(map.size <= 2){
     val result1 = click_ration.map(x => (x._1,formater.format(x._2)))
      result1.mkString(",")
    }else{
      var otherRation = 1D
      click_ration.foreach(x => otherRation -= x._2 )
      val result2 = click_ration :+ "其他"->otherRation
      val result3 = result2.map(x => (x._1,formater.format(x._2)))
      result3.mkString(",")
    }

  }
}
