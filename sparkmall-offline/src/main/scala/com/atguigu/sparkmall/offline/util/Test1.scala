package com.atguigu.sparkmall.offline.util

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

/**
  *
  *
  * @author lijun
  * @date 2018/11/10
  */
object Test1 {

  def main(args: Array[String]): Unit = {
    //    println(Math.round(10.222222222))
    //    println(Math.round(10.525222222))
    //    val bg: BigDecimal = BigDecimal.decimal(1.226)
    //
    //    val f1: Double = bg.setScale(2, RoundingMode.HALF_UP).doubleValue
    //    println(f1)

    //    val list1 = Array(1, 2, 3, 4)
    //    val list2 = Array(4, 5, 6)
    //    var flag = false
    //    for (clickCatrgoryId <- list1) {
    //      if (list2.contains(clickCatrgoryId)) {
    //        flag = true
    //      }
    //    }
    //    println(flag)
    val map: HashMap[String, Long] = HashMap[String, Long]()
    val array = Array(1, 2, 3)

    val buffer: ArrayBuffer[Int] = ArrayBuffer[Int]()
    buffer.appendAll(array)
    //    map.("a", 1)
  }
}
