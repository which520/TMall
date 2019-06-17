package com.which.TMall.mock.util

import scala.collection.mutable.ListBuffer

/**
  * 根据提供的值和比重，来创建RandomOptions对象
  * 然后可以通过getRandomOption来获取一个随机的预定义值
  */
object RandomOptions {
  def apply[T](opt:(T, Int),opts2:(T,Int)*) ={
    var opts = new ListBuffer[(T,Int)]() ++ opts2
    opts.append(opt)
    val randomOptions = new RandomOptions[T]()
    randomOptions.totalWeight=(0 /: opts)(_ + _._2)//计算出总的比重
    opts.foreach{
      case (value,weight) => randomOptions.options ++= (1 to weight).map( _=>value)
    }
    randomOptions
  }

  def main(args: Array[String]): Unit = {
    val opts = RandomOptions(("张三",10),("李四",5))
    (0 to 20).foreach(_=> println(opts.getRandomOption()))
  }
}
class RandomOptions[T]{
  var totalWeight:Int=_
  var options = ListBuffer[T]()

  /**
    * 获取随机的Option的值
    * @return
    */
  def getRandomOption()={
    options(RandomNumUtil.randomInt(0,totalWeight-1))
  }
}