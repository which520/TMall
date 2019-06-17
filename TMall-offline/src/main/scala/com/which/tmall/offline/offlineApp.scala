package com.which.tmall.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.which.TMall.common.bean.UserVisitAction
import com.which.TMall.common.util.ConfigurationUtil
import com.which.tmall.app.{AreaClickTop3App, CategoryTop10App, RangeConversionApp, SessionCategoryTop10}
import com.which.tmall.offline.bean.Condition
import org.apache.spark.sql.SparkSession



object offlineApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("offlineApp")
      .enableHiveSupport()
      .getOrCreate()
    val taskId = UUID.randomUUID().toString
    //val userVisitActionRdd = readUserVisitActionRdd(spark,readConditions)
    //val CategoryTop10 = CategoryTop10App.stataCategoryTop10(spark,userVisitActionRdd,taskId)
    //SessionCategoryTop10.cSessionTop10(taskId,spark,userVisitActionRdd,CategoryTop10)
    //RangeConversionApp.calcPageConversion(taskId,spark,readConditions.targetPageFlow,userVisitActionRdd)
    AreaClickTop3App.startAreaClickTop3Product(spark)
  }

  /**
    * 读取指定条件的userVisitAction
    * @param spark
    * @param condition 条件
    */

  def readUserVisitActionRdd(spark: SparkSession,condition:Condition) ={
    var sql =
      s"""
         |select v.* from user_visit_action v join user_info u on v.user_id = u.user_id
         |where 1 = 1
       """.stripMargin

    if (isNotEmpty(condition.startDate)){
      sql += s" and v.date >= '${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)){
      sql += s" and v.date <= '${condition.endDate}'"
    }
    if(condition.startAge > 0){
      sql += s" and u.age >= ${condition.startAge}"
    }
    if(condition.endAge > 0){
      sql += s" and u.age <= ${condition.endAge}"
    }
    import  spark.implicits._
    spark.sql("use TMall")
    spark.sql(sql).as[UserVisitAction].rdd
  }

  /**
    * 读取过滤条件
    * @return condition
    */
  def readConditions={

    /*读取配置文件*/
    val config = ConfigurationUtil("Conditions.properties")
    /*读取到其中的json字符串*/
    val conditionString = config.getString("condition.params.json")
    /*解析成对应的condition对象*/
    JSON.parseObject(conditionString,classOf[Condition])
  }
}
