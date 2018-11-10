package com.atguigu.sparkmall2018.offline.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  *
  * @author lijun
  * @date 2018/11/9
  */
object SessionStatApp {

  class SessionInfo {

    /** *
      *
      * @param sessionId        sessionId
      * @param startTime        访问开始时间
      * @param searchKeywords   搜索的单词
      * @param clickCategoryIds 点击的商品类别
      * @param stepLength       访问次数
      * @param visitLength      访问时长
      */
    case class SessionInfo(sessionId: String, startTime: String, searchKeywords: String, clickCategoryIds: String, stepLength: Long, visitLength: Long) {

    }

  }


  /**
    * 统计出符合筛选条件的session中，访问时长在小于10s含、10s以上各个范围内的session数量占比。访问步长在小于等于5，和大于5次的占比
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Mock").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val conditionJson: String = ConfigurationUtil("condition.properties").config.getString("condition.params.json")

    val conditionJsonObj: JSONObject = JSON.parseObject(conditionJson)

    //1、从hive中获取访问日志,生成RDD
    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession, conditionJsonObj)
    userVisitActionRDD.foreach(println)
    //2、根据sessionId聚合成集合
    val userVisitActionIterableRDD: RDD[(String, Iterable[UserVisitAction])] = aggrVisitActionsBySessionRDD(userVisitActionRDD)

    //3、计算单个session的统计信息

    //4、遍历所有session，分类累加，聚合信息

  }

  def aggrVisitActionsBySessionRDD(userVisitActionRDD: RDD[UserVisitAction]): RDD[(String, Iterable[UserVisitAction])] = {
    val userVisitActionMapRDD: RDD[(String, UserVisitAction)] = userVisitActionRDD.map(userVisitAction => (userVisitAction.session_id, userVisitAction))
    val userVisitActionIterableRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionMapRDD.groupByKey()
    userVisitActionIterableRDD
  }

  /**
    * 获取访问日志RDD
    *
    * @param sparkSession     连接
    * @param conditionJsonObj 筛选条件
    * @return
    */
  def readUserVisitActionToRDD(sparkSession: SparkSession, conditionJsonObj: JSONObject): RDD[UserVisitAction] = {

    val sql: StringBuilder = StringBuilder.newBuilder

    /**
      * select v.*
      * from user_visit_action v left join user_info u  on v.user_id=u.user_id
      * where action_time>'2018-11-01' and action_time<'2018-12-28'
      * and u.age>20 and u.age<50
      * and professional in ("程序员","学生")
      * and city_id=18
      * and gender="男"
      * and  search_keyword in ("苹果")
      * and click_category_id in (14)
      * and page_id in (1,2,3,4,5,6,7)
      * limit 30;
      */
    sql.append("select v.* ")
      .append("from user_visit_action v left join user_info u  on v.user_id=u.user_id")
      .append(" where 1 = 1")

    if (!conditionJsonObj.getString("startDate").isEmpty) {
      sql.append(" and action_time>'" + conditionJsonObj.getString("startDate") + "'")
    }
    if (!conditionJsonObj.getString("endDate").isEmpty) {
      sql.append(" and action_time<'" + conditionJsonObj.getString("endDate") + "'")
    }
    if (!conditionJsonObj.getString("startAge").isEmpty) {
      sql.append(" and u.age>" + conditionJsonObj.getString("startAge"))
    }
    if (!conditionJsonObj.getString("endAge").isEmpty) {
      sql.append(" and u.age<" + conditionJsonObj.getString("endAge"))
    }
    if (!conditionJsonObj.getString("professionals").isEmpty) {
      sql.append(" and professional in (" + conditionJsonObj.getString("professionals") + ")")
    }
    if (!conditionJsonObj.getString("city").isEmpty) {
      sql.append(" and city_id = " + conditionJsonObj.getString("city"))
    }
    if (!conditionJsonObj.getString("gender").isEmpty) {
      sql.append(" and gender = " + conditionJsonObj.getString("gender"))
    }
    if (!conditionJsonObj.getString("keywords").isEmpty) {
      sql.append(" and search_keyword in( " + conditionJsonObj.getString("keywords") + ")")
    }
    if (!conditionJsonObj.getString("categoryIds").isEmpty) {
      sql.append(" and click_category_id in (" + conditionJsonObj.getString("categoryIds") + ")")
    }
    if (!conditionJsonObj.getString("targetPageFlow").isEmpty) {
      sql.append(" and page_id in (" + conditionJsonObj.getString("targetPageFlow") + ") ")
    }

    sql.append(" limit 30")
    println(sql.toString())
    sparkSession.sql("use " + ConfigurationUtil("config.properties").config.getString("hive.database"))

    import sparkSession.implicits._
    val visitDF: DataFrame = sparkSession.sql(sql.toString())
    visitDF.as[UserVisitAction].rdd
  }

}
