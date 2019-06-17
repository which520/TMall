package com.which.TMall.mock.util

import scala.collection.mutable
import scala.util.Random

/**
  * 生产随机数据的工具
  */
object RandomNumUtil {
  /*随机数生成器对象*/
  private val random = new Random()

  /**
    * @param from
    * @param to
    * @return
    */
  def randomInt(from:Int,to:Int)={
  if(from > to) throw new IllegalArgumentException(s"from 不能大于 to")
  else random.nextInt(to - from + 1) + from
  }

  /**
    * 创建多个Int值
    * @param from
    * @param to
    * @param count  创建的值的个数
    * @param canRepeat 创建的值是否重复
    * @return List[Int] 集合
    */

  def randomMultiInt(from:Int,to:Int,count:Int,canRepeat:Boolean)={
    if(canRepeat){
      (1 to count).toList.map(_ => randomInt(from,to))
    }else{
      val set = mutable.Set[Int]()
      while (set.size<count){
        set+=randomInt(from,to)
      }
      set.toList
    }
  }

  /**
    * 生成一个随机的Long值 范围：【from , to】
    * @param from
    * @param to
    * @return
    */
  def randomLong(from:Long,to:Long)={
    if(from > to) throw new IllegalArgumentException(s"from:$from 不能大于 to;$to")
    else math.abs(random.nextLong())%(to - from + 1) + from
  }
}
