package com.which.tmall.app

import com.which.TMall.common.bean.UserVisitAction
import com.which.TMall.common.util.JDBCUtil
import com.which.tmall.offline.bean.{CategoryCountInfo, SeeionCountInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SessionCategoryTop10 {
  def cSessionTop10(taskId: String, spark: SparkSession, userVisitorActionRdd: RDD[UserVisitAction], categoryTop10Rdd:List[CategoryCountInfo]) = {
    val categoryTop10 = categoryTop10Rdd.map(x => x.categoryId)
    val categoryTop10BD = spark.sparkContext.broadcast(categoryTop10)
    val clickUserActionTop10 = userVisitorActionRdd.filter(x => categoryTop10BD.value.contains(x.click_category_id+""))
    val sessionCount = clickUserActionTop10.map(x => ((x.click_category_id, x.session_id), 1L)).reduceByKey(_ + _)
    val sessionCount2 = sessionCount.map(x => (x._1._1, (x._1._2, x._2))).groupByKey
    val SessionSorted = sessionCount2.flatMap {
      case (cid, it) => {
        it.toList.sortBy(x => x._2)(Ordering.Long.reverse).take(10).map(item => SeeionCountInfo(taskId, cid.toString, item._1, item._2)
        )
      }
    }
    //println(SessionSorted.collect().mkString("\n"))
    val SessionCountinfoArray = SessionSorted.collect().map(item => {
      Array(item.taskId, item.categoryId, item.sessionId, item.clickCount)
    })
    JDBCUtil.executeBatchUpDate("insert into category_top10_session_count values(?,?,?,?)",SessionCountinfoArray)
  }
}
