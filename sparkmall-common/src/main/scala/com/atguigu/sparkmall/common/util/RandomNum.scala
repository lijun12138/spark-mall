package com.atguigu.sparkmall.common.util

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  *
  *
  * @author lijun
  * @date 2018/11/9
  */
object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    // 用delimiter分割 canRepeat为false则不允许重复

    if (!canRepeat && amount > (toNum - fromNum)) {
      println("没有这样的数")
    }
    val seqToSet: ArrayBuffer[Int] = ArrayBuffer[Int]()

    var flag = true

    do {

      val random: Int = fromNum + new Random().nextInt(toNum - fromNum + 1)
      if (canRepeat) {
        seqToSet.append(random)
      } else {
        if (!seqToSet.contains(random)) {
          seqToSet.append(random)
        }
      }
      if (seqToSet.length == amount) {
        flag = false
      }
    } while (flag)

    var result: String = ""
    result = seqToSet.mkString(delimiter)
    result
  }

  def main(args: Array[String]): Unit = {
    println(multi(1, 10, 9, "-", false))
  }


}
