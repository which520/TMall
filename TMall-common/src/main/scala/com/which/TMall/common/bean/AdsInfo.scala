package com.which.TMall.common.bean

import java.text.SimpleDateFormat

case class AdsInfo(
                    timestamp:Long,
                    area:String,
                    city:String,
                    userid:Long,
                    adid:Long
                  ){
  val dayStrign: String = new SimpleDateFormat("yyyy-MM-dd").format(timestamp)
}
