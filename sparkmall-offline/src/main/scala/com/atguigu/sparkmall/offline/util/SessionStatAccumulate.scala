package com.atguigu.sparkmall.offline.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  *
  *
  * @author lijun
  * @date 2018/11/10
  */
class SessionStatAccumulate extends AccumulatorV2[String, HashMap[String, Long]] {
  var sessionVisitMap = new HashMap[String, Long]()

  override def isZero: Boolean = sessionVisitMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val sessionStatAccumulate = new SessionStatAccumulate()
    sessionStatAccumulate.sessionVisitMap ++= sessionVisitMap
    sessionStatAccumulate
  }

  override def reset(): Unit = {
    sessionVisitMap = new HashMap[String, Long]()
  }

  //访问时长在小于10s含、10s以上各个范围内的session数量。访问步长在小于等于5，和大于5次
  override def add(v: String): Unit = {
    val count: Long = sessionVisitMap.getOrElse(v, 0L) + 1L
    sessionVisitMap(v) = count
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    other match {
      case sessionStatAccumulate: SessionStatAccumulate => {
        sessionStatAccumulate.sessionVisitMap.foldLeft(this.sessionVisitMap) {
          case (aggrMap, (key, value)) =>
            val count: Long = aggrMap.getOrElse(key, 0L) + value
            aggrMap(key) = count
            aggrMap
        }
      }
    }

  }

  override def value: mutable.HashMap[String, Long] = this.sessionVisitMap
}
