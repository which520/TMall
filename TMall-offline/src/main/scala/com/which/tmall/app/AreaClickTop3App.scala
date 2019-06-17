package com.which.tmall.app

import java.util.Properties

import com.which.TMall.common.util.ConfigurationUtil
import com.which.tmall.UDF.CityClickCountUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaClickTop3App {
  def startAreaClickTop3Product(spark: SparkSession)={
    spark.udf.register("city_remark",new CityClickCountUDAF)
    spark.sql("use tmall")
    spark.sql(
      s"""
        |select
        |       c.*,
        |       u.click_product_id
        |       from
        |           city_info c
        |       join
        |           user_visit_action u
        |       on
        |           c.city_id = u.city_id
        |       where u.click_product_id != -1
      """.stripMargin).createOrReplaceTempView("t1")
    spark.sql(
      s"""
         |select
         |      area,
         |      click_product_id,
         |      count(click_product_id) click_count,
         |      city_remark(city_name) city_ration
         |      from t1 group by area,click_product_id
       """.stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select * , ROW_NUMBER() over(partition by area order by click_count desc) click_rank from t2
      """.stripMargin).createOrReplaceTempView("t3")
    spark.sql(
      """
        |select
        |     area,
        |     product_name,
        |     click_count,
        |     city_ration
        |   from
        |       t3
        |   join
        |       product_info p
        |   on
        |       t3.click_product_id = p .product_id
        |   where
        |       click_rank <= 3
      """.stripMargin).createOrReplaceTempView("t4")
    val conf = ConfigurationUtil("config.properties")
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    spark.sql(
      """
        |select * from t4
      """.stripMargin)
      .write.mode(SaveMode.Overwrite)
      .jdbc(conf.getString("jdbc.url"),"area_click_top10",props)
  }
}
