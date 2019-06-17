package com.which.TMall.mock.util

import java.util.Date

object RandomDate {
  def apply(startDate:Date,stopDate:Date,step:Int) = {
    val randomDate = new RandomDate
    val avgStepTime = (stopDate.getTime-startDate.getTime)/step
    randomDate.maxStepTime = 4 * avgStepTime
    randomDate.lastDateTime = startDate.getTime
    randomDate
  }


}

class RandomDate{
  //上次action的时间
  var lastDateTime:Long = _
  //每次最大的步长时间
  var maxStepTime:Long = _

  def getRandomDate={
    val timeStep = RandomNumUtil.randomLong(0,maxStepTime)
    lastDateTime += timeStep
    new Date(lastDateTime)
  }


}
