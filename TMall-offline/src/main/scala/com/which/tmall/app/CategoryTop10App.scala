package com.which.tmall.app

import com.sun.javafx.collections.MappingChange.Map
import com.which.TMall.common.bean.UserVisitAction
import com.which.TMall.common.util.JDBCUtil
import com.which.tmall.acc.CidTop10Acc
import com.which.tmall.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map
import scala.collection.immutable.Map

object CategoryTop10App {
  def stataCategoryTop10(spark: SparkSession,userVisitActionRDD:RDD[UserVisitAction] ,taskId:String)={
    val acc = new CidTop10Acc
    spark.sparkContext.register(acc,"cidtop10Acc")

    userVisitActionRDD.foreach{
      visitAction => {
        if(visitAction.click_category_id != -1){
          acc.add(visitAction.click_category_id.toString,"click")
        }else if(visitAction.order_category_ids != null){
          visitAction.order_category_ids.split(",").foreach{
            oid => acc.add(oid,"order")
          }
        }else if (visitAction.pay_category_ids != null){
          visitAction.pay_category_ids.split(",").foreach{
            pid => acc.add(pid,"pay")
          }
        }
      }
    }
    val categoryIdCount = acc.value.groupBy(x=>x._1._1)
    val sortedCategoryTop10 = categoryIdCount.map(kv => new CategoryCountInfo(
      taskId,
      kv._1,
      kv._2.getOrElse((kv._1, "click"), 0L),
      kv._2.getOrElse((kv._1, "order"), 0L),
      kv._2.getOrElse((kv._1, "pay"), 0L)
    )).toList.sortBy(info => (info.clickCount, info.orderCount, info.payCount))(Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse)).take(10)
    //val argList = sortedCategoryTop10.map(info => Array(info.taskId,info.categoryId,info.clickCount,info.orderCount,info.payCount))
    //JDBCUtil.executeBatchUpDate("insert into category_top10 values (?,?,?,?,?)",argList)
    sortedCategoryTop10
  }
}
