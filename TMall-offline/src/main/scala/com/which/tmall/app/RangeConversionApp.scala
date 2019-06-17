package com.which.tmall.app

import com.which.TMall.common.bean.UserVisitAction
import com.which.TMall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RangeConversionApp {
  def calcPageConversion(taskId:String ,spark: SparkSession,targetPageFlow:String,userVisitActionRDD:RDD[UserVisitAction]):Unit={
    val pageFlows = targetPageFlow.split(",")
    val targetPageJumpFlow = pageFlows.slice(0,pageFlows.length -1).zip(pageFlows.slice(1,pageFlows.length))
    val ActionSorted = userVisitActionRDD.groupBy(x=>x.session_id)
      .flatMap{
              case(sid,it)=>
                it.toList.sortBy(x=>x.action_time).map(x=> x.page_id).slice(0,it.size-1)
                  .zip(it.toList.sortBy(x=>x.action_time).map(x=>x.page_id).slice(1,it.size+1))
                  .map(x => (x,1L))}
      .filter(x => targetPageJumpFlow.contains((x._1._1.toString,x._1._2.toString)))
      .reduceByKey(_+_)
    val pageCount = userVisitActionRDD.filter(x => pageFlows.contains(x.page_id.toString))
      .map(x => (x.page_id,1L)).reduceByKey(_+_).collect().toMap
    val RationJumpPage = ActionSorted.map({
      case ((a, b), c) => (s"$a->$b", ((c.toDouble / pageCount(a.toLong)) * 100).formatted("%.2f%%"))})
    println(RationJumpPage.collect().mkString("\n"))
    val RationArray = RationJumpPage.collect().toList.map(x =>Array[Any](taskId,x._1,x._2))
    JDBCUtil.executeBatchUpDate("insert into page_conversion_rate values (?,?,?)" , RationArray)
  }
}
