package com.atguigu.sparkmall.common.util

import scala.collection.mutable.ArrayBuffer

/**
  *
  *
  * @author lijun
  * @date 2018/11/9
  */
object RandomOptions {

  def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
    val randomOpt = new RandomOptions[T]
    for (elem <- opts) {
      randomOpt.totalWeight += elem.weight
      for (i <- 0 until elem.weight) {
        randomOpt.listBuffer.append(elem)
      }

    }
    // 传入多个RanOpt对象，初始化选项池
    randomOpt
  }


  def main(args: Array[String]): Unit = {
    val randomName = RandomOptions(RanOpt("zhangchen", 10), RanOpt("li4", 30))
    for (i <- 1 to 10) {
      println(i + ":" + randomName.getRandomOpt())

    }
  }


}

//随机选项
case class RanOpt[T](value: T, weight: Int) {
}

class RandomOptions[T](opts: RanOpt[T]*) {
  var totalWeight: Int = _
  val listBuffer: ArrayBuffer[RanOpt[T]] = ArrayBuffer[RanOpt[T]]()

  def getRandomOpt(): T = {
    listBuffer(RandomNum(0, listBuffer.length - 1)).value
  }

}
