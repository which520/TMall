package com.which.tmall.offline.bean

/**
  *
  * @param startDate 起始日期
  * @param endDate    结束日期
  * @param startAge   开始年龄
  * @param endAge     结束年龄
  * @param professionals 职业
  * @param city       城市
  * @param gender     性别
  * @param keywords   关键字
  * @param categoryIds 品类id
  * @param targetPageFlow 目标页
  */
case class Condition(var startDate: String,
                     var endDate: String,
                     var startAge: Int,
                     var endAge: Int,
                     var professionals: String,
                     var city: String,
                     var gender: String,
                     var keywords: String,
                     var categoryIds: String,
                     var targetPageFlow: String
                    )
case class CategoryCountInfo(
                            taskId:String,
                            categoryId:String,
                            clickCount:Long,
                            orderCount:Long,
                            payCount:Long
                            )

case class SeeionCountInfo(
                            taskId: String,
                            categoryId: String,
                            sessionId: String,
                            clickCount: Long
                          )

