package com.atguigu.sparkmall.offline.app

import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.model.{ProductInfo, UserVisitAction}
import com.atguigu.sparkmall.common.util.{ConfigurationUtil, RandomNum}
import com.atguigu.sparkmall.offline.model.{HotProductUser, PageConvertRate, _}
import com.atguigu.sparkmall.offline.util.{CategoryStatAccumulate, ProudctStatAccumulate, SessionStatAccumulate, UDFGroupByCityCount}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

/**
  *
  *
  * @author lijun
  * @date 2018/11/9
  */
object SessionStatApp {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Offline").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //注册累加器
    val accumulate = new SessionStatAccumulate
    val accumulate2 = new CategoryStatAccumulate

    val accumulate3 = new ProudctStatAccumulate

    sparkSession.sparkContext.register(accumulate)
    sparkSession.sparkContext.register(accumulate2)
    sparkSession.sparkContext.register(accumulate3)

    //任务id
    val taskId: String = UUID.randomUUID().toString

    val conditionJson: String = ConfigurationUtil("condition.properties").config.getString("condition.params.json")

    val conditionJsonObj: JSONObject = JSON.parseObject(conditionJson)


    //1、从hive中获取访问日志,生成RDD
    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession, conditionJsonObj)
    //2、根据sessionId聚合成集合
    val userVisitActionIterableRDD: RDD[(String, Iterable[UserVisitAction])] = aggrVisitActionsBySessionRDD(userVisitActionRDD)

    //3、计算单个session的统计信息
    val sessionInfoRDD: RDD[(String, SessionInfo)] = getSessionInfo(userVisitActionIterableRDD, taskId)
    //
    sessionInfoRDD.cache()


    //需求一：
    demandOne(sparkSession, sessionInfoRDD, accumulate, taskId)
    //需求二：按每小时session数量比例随机抽取1000个session
    demandTwo(sparkSession, sessionInfoRDD, accumulate, taskId)
    //需求三：获取点击、下单和支付数量排名前 10 的品类
    val categoryTopNs: Array[CategoryTopN] = demandThree(sparkSession, sessionInfoRDD, accumulate2, taskId)
    //需求四：Top10 热门品类中 Top10 活跃 Session 统计
    demandFour(sparkSession, categoryTopNs, userVisitActionRDD, accumulate2, taskId)

    //需求五：计算页面单跳转化率
    demandFive(sparkSession, userVisitActionRDD, taskId)

    //需求六：各区域  点击量Top3 商品统计
    demandSix(sparkSession, taskId)

    /*
     * 练习一：求：点击过同一商品两次以上各个用户职业的占比情况。
     * 结果表字段：
     * 用户总数，老师占比，程序员占比，项目经理占比，学生占比
     */

    //    exerciseOne(sparkSession, userVisitActionRDD, accumulate, taskId)


    //    exerciseTwo(sparkSession, sessionInfoRDD, userVisitActionRDD, accumulate3, taskId)


    sparkSession.stop()

  }

  /**
    * 各区域  点击量Top3 商品统计
    *
    * @param sparkSession
    * @param userVisitActionRDD
    * @param taskId
    * @return
    */
  def demandSix(sparkSession: SparkSession, taskId: String) = {

    sparkSession.udf.register("groupby_city_count", new UDFGroupByCityCount)

    sparkSession.sql("use sparkmall ")
    val basisDF: DataFrame = sparkSession.sql("select v.click_product_id,c.area,c.city_name from user_visit_action v join city_info c on v.city_id=c.city_id where v.click_product_id>0 ")

    basisDF.createOrReplaceTempView("basis")

    //以地区和商品id为key，统计出点击次数
    val countDF: DataFrame = sparkSession.sql("select click_product_id,area,count(*) count ,groupby_city_count(city_name) city_remark from  basis group by click_product_id,area")

    //统计出每个地区的前三名
    countDF.createOrReplaceTempView("count_table")
    val rankDF: DataFrame = sparkSession.sql("select click_product_id,area,count,city_remark,row_number() over(partition by area order by count desc) rank from count_table ")
    rankDF.createOrReplaceTempView("count_rank_table")


    //统计前三
    sparkSession.sql("select area,product_name,count,rank,city_remark from count_rank_table c join product_info p on c.click_product_id=p.product_id  where rank<=3").show(100, false)

  }

  /**
    * 计算页面单跳转化率
    *
    * @param sparkSession
    * @param userVisitActionRDD
    * @param taskId
    * @return
    */
  def demandFive(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {

    val condition: String = ConfigurationUtil("condition.properties").config.getString("condition.params.json")
    val conditionObj: JSONObject = JSON.parseObject(condition)
    val targetPageIds: Array[String] = conditionObj.getString("targetPageFlow").split(",")

    val targetPageIdsBD: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageIds)

    val fromIds: Array[String] = targetPageIds.slice(0, 6).zip(targetPageIds.slice(1, 7)).map {
      case (id1, id2) => {
        id1 + "_" + id2
      }
    }

    val fromIdsBD: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(fromIds)

    //计算每个页面的点击次数
    val pageIdCount: collection.Map[Long, Long] = userVisitActionRDD.filter { userVisitActionRDD => {
      targetPageIdsBD.value.contains(userVisitActionRDD.page_id.toString)
    }
    }.map { userVisitActionRDD => (userVisitActionRDD.page_id, 1L) }.countByKey()
    val pageIdCountBD: Broadcast[collection.Map[Long, Long]] = sparkSession.sparkContext.broadcast(pageIdCount)


    val sessionRDD: RDD[(String, UserVisitAction)] = userVisitActionRDD.map {
      userVisitAction => {
        (userVisitAction.session_id, userVisitAction)
      }
    }

    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionRDD.groupByKey()

    //找到符合规则的页面跳转
    val pageJumpsRDD: RDD[(String, Long)] = sessionGroupRDD.flatMap {
      case (sessionId, userVisitActions) => {

        val sortUservisit: Array[UserVisitAction] = userVisitActions.toArray.sortWith {
          case (userVisitAction1, userVisitAction2) => {
            val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            format.parse(userVisitAction1.action_time).getTime < format.parse(userVisitAction2.action_time).getTime
          }
        }

        val tuplesZip: Array[(UserVisitAction, UserVisitAction)] = sortUservisit.slice(0, sortUservisit.length - 1).zip(sortUservisit.slice(1, sortUservisit.length))

        val sessionPageJump: Array[String] = tuplesZip.map { case (action1, action2) => (action1.page_id + "_" + action2.page_id) }

        val sessionPageJumpCount: Array[(String, Long)] = sessionPageJump.filter { pageJump => {
          fromIdsBD.value.contains(pageJump)
        }
        }.map(pageJump => (pageJump, 1L))

        sessionPageJumpCount
      }
    }

    val pageJumpsCountMap: collection.Map[String, Long] = pageJumpsRDD.countByKey()


    val pageConvertRateArray: Array[PageConvertRate] = pageJumpsCountMap.map {
      case (fromPageId, count) => {

        val fromPage: String = fromPageId.split("_")(0)
        val pageCount: Long = pageIdCountBD.value.get(fromPage.toLong).get

        val ratio: Double = count / pageCount.toDouble * 1000 / 10.0
        PageConvertRate(taskId, fromPageId, BigDecimal.decimal(ratio).setScale(2, RoundingMode.HALF_UP).doubleValue)
      }
    }.toArray

    import sparkSession.implicits._
    val dataFrame: DataFrame = sparkSession.sparkContext.makeRDD(pageConvertRateArray).toDF()
    val tableName = "page_convert_rate"
    insertIntoMysql(tableName, dataFrame)
    println("需求五已完成")

  }

  /**
    * 求： 列出购买过10大热门商品的较多的用户的双十一当日的访问记录
    * 注： 每个热门商品选2个购买该商品最多的用户，列出他们当日所有访问明细用时间排序
    * 热门商品以点击量为准。
    *
    * @param sparkSession
    * @param sessionInfoRDD
    * @param userVisitActionRDD
    * @param accumulate
    * @param taskId
    */
  def exerciseTwo(sparkSession: SparkSession, sessionInfoRDD: RDD[(String, SessionInfo)], userVisitActionRDD: RDD[UserVisitAction], accumulate: ProudctStatAccumulate, taskId: String) = {

    //求出热门商品
    sessionInfoRDD.foreach {
      case (sessionId, sessionInfo) => {
        val clickProductIds: String = sessionInfo.clickProductIds


        if (clickProductIds != null && !clickProductIds.isEmpty) {
          val clickIds: Array[String] = clickProductIds.split(",")
          for (elem <- clickIds) {
            accumulate.add(elem + "_" + "proudct")
          }
        }
      }
    }

    val counts: mutable.HashMap[String, Long] = accumulate.value

    //cid,map(cid_click,count)
    val categoryCountMap: Map[String, mutable.HashMap[String, Long]] = counts.groupBy {
      case (key, count) => {
        val pid: String = key.split("_")(0)
        pid
      }
    }

    val proudctTopNs: immutable.Iterable[ProudctTopN] = categoryCountMap.map {
      case (pid, categoryCountSet) => {
        val clickCount: Long = categoryCountSet.getOrElse(pid + "_" + "proudct", 0L)
        ProudctTopN(taskId, pid, clickCount)
      }
    }
    val proudctTop: Array[ProudctTopN] = proudctTopNs.toArray.sortWith {
      case (product1, proudct2) => {
        if (product1.click_count > proudct2.click_count) {
          true
        } else
          false
      }
    }.take(10)


    //求出每个热门商品的头号粉丝
    val proudctIds: Array[String] = proudctTop.map(_.proudct_id)
    val proudctIdsBD: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(proudctIds)


    val userVisitActionFilterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
      case userVisitAction => {
        var flag = false
        if (userVisitAction.pay_product_ids != null && !userVisitAction.pay_product_ids.isEmpty) {
          val payProudctIds: Array[String] = userVisitAction.pay_product_ids.split(",")
          for (elem <- payProudctIds) {
            if (proudctIdsBD.value.contains(elem)) {
              flag = true
            }
          }
        }
        flag
      }
    }

    val sessionIdAndProudctIdRDD: RDD[(String, Long)] = userVisitActionFilterRDD.flatMap {
      case userVisitAction => {
        val map = new mutable.HashMap[String, Long]()
        val payProudctIds: Array[String] = userVisitAction.pay_product_ids.split(",")
        for (elem <- payProudctIds) {
          map += (userVisitAction.user_id + "_" + elem -> 1L)
        }
        map
      }
    }

    val countRDD: RDD[(String, Long)] = sessionIdAndProudctIdRDD.reduceByKey(_ + _)

    val proudctIdKeyRDD: RDD[(String, UserProudctTopN)] = countRDD.map {
      case (id, count) => {
        val ids: Array[String] = id.split("_")
        val userId: String = ids(0)
        val proudctId: String = ids(1)
        (proudctId, UserProudctTopN(taskId, userId, proudctId, count))
      }
    }

    val proudctIdKeyGroupRDD: RDD[(String, Iterable[UserProudctTopN])] = proudctIdKeyRDD.groupByKey()

    val topNMapRDD: RDD[(String, UserProudctTopN)] = proudctIdKeyGroupRDD.flatMap {
      case (proudctId, userProudctTopN) => {

        val topNMap = new mutable.HashMap[String, UserProudctTopN]()

        val userProudctTopNs: Array[UserProudctTopN] = userProudctTopN.toArray.sortWith {
          case (userProudctTopN1, userProudctTopN2) => {
            if (userProudctTopN1.count > userProudctTopN2.count) {
              true
            } else
              false
          }
        }.take(2)

        for (elem <- userProudctTopNs) {
          topNMap += (elem.userId -> elem)
        }
        topNMap
      }
    }

    val userIdRDD: RDD[(String, UserVisitAction)] = userVisitActionFilterRDD.flatMap {
      case userVisitAction => {
        val map = new mutable.HashMap[String, UserVisitAction]()
        val payProudctIds: Array[String] = userVisitAction.pay_product_ids.split(",")
        for (elem <- payProudctIds) {
          map += (userVisitAction.user_id.toString -> userVisitAction)
        }
        map
      }
    }
    val userIdAndVisitACtion: RDD[(String, (UserProudctTopN, UserVisitAction))] = topNMapRDD.join(userIdRDD)


    val hotProductUserRDD: RDD[HotProductUser] = userIdAndVisitACtion.map {
      case (userId, (userProudctTopN, userVisitAction)) => {


        val proudctId: String = userProudctTopN.proudctId
        var product_name: String = userVisitAction.product_name
        val user_name: String = userVisitAction.user_name
        val user_id: String = userVisitAction.user_id.toString
        val action_time: String = userVisitAction.action_time

        var operation_type: String = ""
        var operation_obj: String = ""

        if (userVisitAction.click_product_id > 0) {
          operation_type = "click"
          operation_obj = userVisitAction.click_product_id.toString
        }
        if (userVisitAction.search_keyword != null && !userVisitAction.search_keyword.isEmpty) {
          operation_type = "search"
          operation_obj = userVisitAction.search_keyword
        }
        if (userVisitAction.order_product_ids != null && !userVisitAction.order_product_ids.isEmpty) {
          operation_type = "order"
          operation_obj = userVisitAction.order_product_ids
        }
        if (userVisitAction.pay_product_ids != null && !userVisitAction.pay_product_ids.isEmpty) {
          operation_type = "pay"
          operation_obj = userVisitAction.pay_product_ids
        }

        HotProductUser(taskId, proudctId.toLong, product_name, user_id, user_name, action_time, operation_type, operation_obj)

      }
    }

    import sparkSession.implicits._
    val dataFrame: DataFrame = hotProductUserRDD.toDF()
    val tableName = "hot_proudct_user_info"
    insertIntoMysql(tableName, dataFrame)

    println("练习二完成")
  }


  def exerciseOne(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], accumulate: SessionStatAccumulate, taskId: String) = {

    //筛选出点击过同一商品两次的RDD
    val userIdVisitRDD: RDD[(String, UserVisitAction)] = userVisitActionRDD.map(UserVisitAction => (UserVisitAction.session_id, UserVisitAction))

    val userIdVisitGroupRDD: RDD[(String, Iterable[UserVisitAction])] = userIdVisitRDD.groupByKey()

    val userIdVisitFilterRDD: RDD[(String, Iterable[UserVisitAction])] = userIdVisitGroupRDD.filter {
      case (sessionId, userVisitAction) => {
        var flag = false
        val productId = new mutable.HashMap[Long, Long]()
        for (elem <- userVisitAction) {
          if (elem.click_product_id > 0) {
            productId(elem.click_product_id) = productId.getOrElse(elem.click_product_id, 0L) + 1L
            if (productId(elem.click_product_id) >= 2) {
              flag = true
            }
          }
        }
        flag
      }
    }

    val sessionInfoRDD: RDD[(String, SessionInfo)] = getSessionInfo(userIdVisitFilterRDD, taskId)

    sessionInfoRDD.foreach {
      case (sessionId, sessionInfo) => {

        //访问用户为老师
        if (sessionInfo.professional == "老师") {
          accumulate.add("session_visit_teacher_ratio")
        }
        //访问用户为程序员
        if (sessionInfo.professional == "程序员") {
          accumulate.add("session_visit_programmer_ratio")
        }
        //访问用户为项目经理
        if (sessionInfo.professional == "经理") {
          accumulate.add("session_step_PM_ratio")
        }
        //访问用户为学生
        if (sessionInfo.professional == "学生") {
          accumulate.add("session_step_stu_ratio")
        }
        accumulate.add("professional_count")
      }
    }

    val professionalCount: mutable.HashMap[String, Long] = accumulate.value


    val session_visit_teacher: Long = professionalCount.getOrElse("session_visit_teacher_ratio", 0L)
    val session_visit_programmer: Long = professionalCount.getOrElse("session_visit_programmer_ratio", 0L)
    val session_step_PM: Long = professionalCount.getOrElse("session_step_PM_ratio", 0L)
    val session_step_stu: Long = professionalCount.getOrElse("session_step_stu_ratio", 0L)
    val session_count: Long = professionalCount.getOrElse("professional_count", 0L)


    val session_visit_teacher_ratio: Double = session_visit_teacher / session_count.toDouble * 10000 / 100.0
    val session_visit_programmer_ratio: Double = session_visit_programmer / session_count.toDouble * 10000 / 100.0
    val session_step_PM_ratio: Double = session_step_PM / session_count.toDouble * 10000 / 100.0
    val session_step_stu_ratio: Double = session_step_stu / session_count.toDouble * 10000 / 100.0


    val professionalStat = ProfessionalStat(taskId, session_count, BigDecimal.decimal(session_visit_teacher_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue, BigDecimal.decimal(session_visit_programmer_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue, BigDecimal.decimal(session_step_PM_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue, BigDecimal.decimal(session_step_stu_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue)

    val tableName = "professional_stat"

    import sparkSession.implicits._
    val dataFrame: DataFrame = sparkSession.sparkContext.makeRDD(Array(professionalStat)).toDF

    insertIntoMysql(tableName, dataFrame)
    println("练习一已完成")
  }

  /**
    * 对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。
    *
    * @param sparkSession
    * @param categoryTopNs
    * @param userVisitActionRDD
    * @param accumulate
    * @param taskId
    */
  def demandFour(sparkSession: SparkSession, categoryTopNs: Array[CategoryTopN]
                 , userVisitActionRDD: RDD[UserVisitAction]
                 , accumulate: CategoryStatAccumulate
                 , taskId: String
                ) = {

    //1.将品类不在top10的sessionInfo过滤
    val categoryTopNsIds: Array[String] = categoryTopNs.map(_.category_id)
    val categoryTopNsIdsBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(categoryTopNsIds)

    val userVisitActionFilterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
      userVisitAction => {
        if (categoryTopNsIdsBC.value.contains(userVisitAction.click_category_id.toString)) {
          true
        } else
          false
      }
    }


    val sessionIdCategoryRDD: RDD[(String, Long)] = userVisitActionFilterRDD.map(userVisitAction => (userVisitAction.session_id + "_" + userVisitAction.click_category_id, 1L))

    val sessionIdCategoryCountRDD: RDD[(String, Long)] = sessionIdCategoryRDD.reduceByKey(_ + _)

    val topSessionPerCidRDD: RDD[(String, TopSessionPerCid)] = sessionIdCategoryCountRDD.map {
      case (sessionIdCategory, count) => {
        val sessionIdAndCategory: Array[String] = sessionIdCategory.split("_")
        val sessionId: String = sessionIdAndCategory(0)
        val categoryId: String = sessionIdAndCategory(1)

        (categoryId, new TopSessionPerCid(taskId, categoryId, sessionId, count))
      }
    }

    val topSessionPerCidGroupRDD: RDD[(String, Iterable[TopSessionPerCid])] = topSessionPerCidRDD.groupByKey


    val topNCategoryRDD: RDD[TopSessionPerCid] = topSessionPerCidGroupRDD.flatMap {
      case (sessionId, topSessionPerCids) => {
        val topNCategory: mutable.ArrayOps[TopSessionPerCid] = topSessionPerCids.toArray.sortWith {
          case (topSessionPerCid1, topSessionPerCid2) => {
            if (topSessionPerCid1.clickCount > topSessionPerCid2.clickCount) {
              true
            } else
              false
          }
        }.take(10)

        topNCategory
      }
    }

    import sparkSession.implicits._

    val topNCategoryDF: DataFrame = topNCategoryRDD.toDF()
    val tableName = "top10_session_per_top10_cid"
    insertIntoMysql(tableName, topNCategoryDF)
    println("需求四完成")


  }

  /**
    * 获取点击、下单和支付数量排名前 10 的品类
    *
    * @param sparkSession
    * @param sessionInfoRDD
    * @param accumulate
    * @param taskId
    */
  def demandThree(sparkSession: SparkSession, sessionInfoRDD: RDD[(String, SessionInfo)], accumulate: CategoryStatAccumulate, taskId: String) = {


    sessionInfoRDD.foreach {
      case (sessionId, sessionInfo) => {
        val clickCategoryId: String = sessionInfo.clickCategoryIds
        val orderCategoryIds: String = sessionInfo.orderCategoryIds
        val payCategoryIds: String = sessionInfo.payCategoryIds


        if (clickCategoryId != null && !clickCategoryId.isEmpty) {
          val clickIds: Array[String] = clickCategoryId.split(",")
          for (elem <- clickIds) {
            accumulate.add(elem + "_" + "click")
          }
        }
        if (orderCategoryIds != null && !orderCategoryIds.isEmpty) {
          val orderIds: Array[String] = orderCategoryIds.split(",")
          for (elem <- orderIds) {
            accumulate.add(elem + "_" + "order")
          }
        }
        if (payCategoryIds != null && !payCategoryIds.isEmpty) {
          val orderIds: Array[String] = payCategoryIds.split(",")
          for (elem <- orderIds) {
            accumulate.add(elem + "_" + "pay")
          }
        }
      }
    }

    //cid_click,count
    val categoryCount: mutable.HashMap[String, Long] = accumulate.value

    //cid,map(cid_click,count)
    val categoryCountMap: Map[String, mutable.HashMap[String, Long]] = categoryCount.groupBy {
      case (key, count) => {
        val cid: String = key.split("_")(0)
        cid
      }
    }

    val categoryTopN: immutable.Iterable[CategoryTopN] = categoryCountMap.map {
      case (cid, categoryCountSet) => {
        val clickCount: Long = categoryCountSet.getOrElse(cid + "_" + "click", 0L)
        val orderCount: Long = categoryCountSet.getOrElse(cid + "_" + "order", 0L)
        val payCount: Long = categoryCountSet.getOrElse(cid + "_" + "pay", 0L)

        new CategoryTopN(taskId, cid, clickCount, orderCount, payCount)
      }
    }
    //按照点击、下单、支付的优先顺序进行排序
    val categoryTopNs: Array[CategoryTopN] = categoryTopN.toArray.sortWith {
      case (categoryTopN1, categoryTopN2) => {
        if (categoryTopN1.click_count > categoryTopN2.click_count) {
          true
        } else if (categoryTopN1.click_count == categoryTopN2.click_count) {
          if (categoryTopN1.order_count > categoryTopN2.order_count) {
            true
          } else if (categoryTopN1.order_count == categoryTopN2.order_count) {
            if (categoryTopN1.pay_count > categoryTopN2.pay_count) {
              true
            } else
              false
          }
          false
        } else {
          false
        }
      }
    }.take(10)

    import sparkSession.implicits._

    val categoryTopNsDF: DataFrame = sparkSession.sparkContext.makeRDD(categoryTopNs).toDF()
    val tableName = "category_top10"
    insertIntoMysql(tableName, categoryTopNsDF)
    println("需求三完成")

    categoryTopNs
  }


  /**
    * 需求二：按每小时session数量比例随机抽取1000个session
    *
    * @param sparkSession
    * @param sessionInfoRDD
    * @param accumulate
    * @return
    */
  def demandTwo(sparkSession: SparkSession, sessionInfoRDD: RDD[(String, SessionInfo)], accumulate: SessionStatAccumulate, taskId: String) = {

    val needSessionNumber = 1000
    val sessionCount: Long = sessionInfoRDD.count()
    //把时间作为k，一个可迭代的RDD集合
    val hourSessionInfoRDD: RDD[(String, SessionInfo)] = sessionInfoRDD.map {
      case (sessionId, sessionInfo) => {
        val hourTime: String = sessionInfo.startTime.split(":")(0)
        (hourTime, sessionInfo)
      }
    }
    val hourSessionInfoGroupRDD: RDD[(String, Iterable[SessionInfo])] = hourSessionInfoRDD.groupByKey()

    //取出相应比例的值
    val randomSessionsRDD: RDD[SessionInfo] = hourSessionInfoGroupRDD.flatMap {
      case (dayHour, sessionInfos) => {
        val count: Int = sessionInfos.size
        val targetNumber: Double = count.toDouble / sessionCount * needSessionNumber

        val sessionList: ArrayBuffer[SessionInfo] = randomExtract(sessionInfos.toArray, targetNumber.toLong)
        sessionList
      }
    }

    import sparkSession.implicits._

    val randomSessionsDF: DataFrame = randomSessionsRDD.toDF()
    val tableName = "random_session_info"
    insertIntoMysql(tableName, randomSessionsDF)
    println("需求二完成")

  }

  /**
    * 获取一比例数量的随机值
    *
    * @param sessionInfos
    * @param targetNumber
    */
  def randomExtract[T](array: Array[T], targetNumber: Long) = {

    val values: ArrayBuffer[T] = ArrayBuffer[T]()
    val indexs: mutable.HashSet[Int] = mutable.HashSet[Int]()

    while (values.size < targetNumber) {
      val index: Int = new Random().nextInt(array.size)
      if (!indexs.contains(index)) {
        values += array(index)
        indexs += index
      }
    }
    values
  }

  /**
    * 需求一:统计出符合筛选条件的session中，
    * 1.访问时长在小于10s含、10s以上各个范围内的session数量占比。
    * 2.访问步长在小于等于5，和大于5次的占比
    *
    * @param sparkSession
    * @param sessionInfoRDD
    * @param accumulate
    */
  def demandOne(sparkSession: SparkSession, sessionInfoRDD: RDD[(String, SessionInfo)], accumulate: SessionStatAccumulate, taskId: String) = {
    //4、遍历所有session，分类累加，聚合信息,访问时长在小于10s含、10s以上各个范围内的session数量占比。访问步长在小于等于5，和大于5次的占比
    accSessionInfo(sessionInfoRDD, accumulate)
    //获取累加器结果
    val sessionStat: SessionStat = statSessionAccumulator(accumulate, taskId)


    val sessionStatRDD: RDD[SessionStat] = sparkSession.sparkContext.makeRDD(Array(sessionStat))
    val tableName = "session_stat"

    import sparkSession.implicits._

    insertIntoMysql(tableName, sessionStatRDD.toDF())
    println("需求一完成")
  }

  def insertIntoMysql(tableName: String, dataFrame: DataFrame) = {
    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config


    dataFrame.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .option("dbtable", tableName)
      .mode(SaveMode.Append)
      .save()


  }

  def statSessionAccumulator(accumulate: SessionStatAccumulate, taskId: String): SessionStat = {
    //筛选条件
    val condition: String = ConfigurationUtil("condition.properties").config.getString("condition.params.json")

    val counts: mutable.HashMap[String, Long] = accumulate.value

    val session_visit_length_le_10s: Long = counts.getOrElse("session_visit_length_le_10s_ratio", 0L)
    val session_visit_length_gt_10s: Long = counts.getOrElse("session_visit_length_gt_10s_ratio", 0L)
    val session_step_length_le_5: Long = counts.getOrElse("session_step_length_le_5_ratio", 0L)
    val session_step_length_gt_5: Long = counts.getOrElse("session_step_length_gt_5_ratio", 0L)
    val session_count: Long = counts.getOrElse("session_count", 0L)


    val session_visit_length_le_10s_ratio: Double = session_visit_length_le_10s / session_count.toDouble * 10000 / 100.0
    val session_visit_length_gt_10s_ratio: Double = session_visit_length_gt_10s / session_count.toDouble * 10000 / 100.0
    val session_step_length_le_5_ratio: Double = session_step_length_le_5 / session_count.toDouble * 10000 / 100.0
    val session_step_length_gt_5_ratio: Double = session_step_length_gt_5 / session_count.toDouble * 10000 / 100.0


    val sessionStat = SessionStat(taskId, condition, session_count, BigDecimal.decimal(session_visit_length_le_10s_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue,
      BigDecimal.decimal(session_visit_length_gt_10s_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue,
      BigDecimal.decimal(session_step_length_le_5_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue,
      BigDecimal.decimal(session_step_length_gt_5_ratio).setScale(2, RoundingMode.HALF_UP).doubleValue)
    sessionStat
  }

  /**
    * 访问时长在小于10s含、10s以上各个范围内的session数量占比
    * 访问步长在小于等于5，和大于5次的占比
    *
    * @param sessionInfoRDD
    * @param accumulate
    * @return
    */
  def accSessionInfo(sessionInfoRDD: RDD[(String, SessionInfo)], accumulate: SessionStatAccumulate): Unit = {
    sessionInfoRDD.foreach {
      case (sessionId, sessionInfo) =>

        //访问时长在小于10s含的数量
        if (sessionInfo.visitLength <= 10) {
          accumulate.add("session_visit_length_le_10s_ratio")
        }
        //访问时长在大于10s含的数量
        if (sessionInfo.visitLength > 10) {
          accumulate.add("session_visit_length_gt_10s_ratio")
        }
        //访问步长在小于等于5
        if (sessionInfo.stepLength <= 5) {
          accumulate.add("session_step_length_le_5_ratio")
        }
        //访问步长在大于5
        if (sessionInfo.stepLength > 5) {
          accumulate.add("session_step_length_gt_5_ratio")
        }
        accumulate.add("session_count")


    }

  }


  /**
    * 求得访问时长，步长,搜索过的商品，下过单的商品，支付过的商品
    *
    * @param userVisitActionIterableRDD
    * @return
    */
  def getSessionInfo(userVisitActionIterableRDD: RDD[(String, Iterable[UserVisitAction])], taskId: String): RDD[(String, SessionInfo)] = {
    userVisitActionIterableRDD.map {
      case (sessionId, iterableUserVisitAction) => {


        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        var minVisitTime = 0L
        var maxVisitTime = 0L
        var stepLength = 0L
        var professional = ""

        val keyWords: ArrayBuffer[String] = new ArrayBuffer[String]()
        val clickCategoryIds: ArrayBuffer[Long] = ArrayBuffer[Long]()
        val clickProductIds: ArrayBuffer[Long] = ArrayBuffer[Long]()
        val orderProductIds: ArrayBuffer[String] = ArrayBuffer[String]()
        val orderCategoryIds: ArrayBuffer[String] = ArrayBuffer[String]()
        val payCategoryIds: ArrayBuffer[String] = ArrayBuffer[String]()
        val payProductIds: ArrayBuffer[String] = ArrayBuffer[String]()


        for (userVisitAction <- iterableUserVisitAction) {
          //获取到访问时长
          val actionTime: Long = format.parse(userVisitAction.action_time).getTime

          if (minVisitTime == 0 || actionTime < minVisitTime) {
            minVisitTime = actionTime
          }
          if (maxVisitTime == 0 || actionTime > maxVisitTime) {
            maxVisitTime = actionTime
          }

          if (userVisitAction.search_keyword != null && !userVisitAction.search_keyword.isEmpty) {
            keyWords += (userVisitAction.search_keyword)
          }
          if (userVisitAction.professional != null && !userVisitAction.professional.isEmpty) {
            professional = (userVisitAction.professional)
          }
          if (userVisitAction.click_category_id > 0) {
            clickCategoryIds += (userVisitAction.click_category_id)
          }
          if (userVisitAction.click_product_id > 0) {
            clickProductIds += (userVisitAction.click_product_id)
          }
          if (userVisitAction.order_product_ids != null && !userVisitAction.order_product_ids.isEmpty) {
            orderProductIds += (userVisitAction.order_product_ids)
          }
          if (userVisitAction.order_category_ids != null && !userVisitAction.order_category_ids.isEmpty) {
            orderCategoryIds += (userVisitAction.order_category_ids)
          }
          if (userVisitAction.pay_category_ids != null && !userVisitAction.pay_category_ids.isEmpty) {
            payCategoryIds += (userVisitAction.pay_category_ids)
          }
          if (userVisitAction.pay_product_ids != null && !userVisitAction.pay_product_ids.isEmpty) {
            payProductIds += (userVisitAction.pay_product_ids)
          }
          //获取步长
          stepLength += 1
        }
        val sessionInfo = SessionInfo(taskId
          , sessionId
          , format.format(minVisitTime)
          , keyWords.mkString(",")
          , clickCategoryIds.mkString(",")
          , stepLength
          , (maxVisitTime - minVisitTime) / 1000
          , clickProductIds.mkString(",")
          , orderCategoryIds.mkString(",")
          , orderProductIds.mkString(",")
          , payCategoryIds.mkString(",")
          , payProductIds.mkString(",")
          , professional
        )
        (sessionId, sessionInfo)
      }
    }
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
      * select v.*,u.professional,u.username user_name, p.product_name
      * from user_visit_action v join user_info u  on v.user_id=u.user_id
      * left join product_info p on v.click_product_id=p.product_id
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
    sql.append("select v.*, u.professional, u.username user_name, p.product_name ")
      .append(" from user_visit_action v join user_info u  on v.user_id=u.user_id")
      .append(" left join product_info p on v.click_product_id=p.product_id ")
      .append(" where 1 = 1")

    if (!conditionJsonObj.getString("startDate").isEmpty) {
      sql.append(" and `date`>= '" + conditionJsonObj.getString("startDate") + "'")
    }
    if (!conditionJsonObj.getString("endDate").isEmpty) {
      sql.append(" and `date` <='" + conditionJsonObj.getString("endDate") + "'")
    }
    if (!conditionJsonObj.getString("startAge").isEmpty) {
      sql.append(" and u.age>=" + conditionJsonObj.getString("startAge"))
    }
    if (!conditionJsonObj.getString("endAge").isEmpty) {
      sql.append(" and u.age<=" + conditionJsonObj.getString("endAge"))
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
    //    if (!conditionJsonObj.getString("targetPageFlow").isEmpty) {
    //      sql.append(" and page_id in (" + conditionJsonObj.getString("targetPageFlow") + ") ")
    //    }

    //    sql.append(" limit 30")
    println(sql.toString())
    sparkSession.sql("use " + ConfigurationUtil("config.properties").config.getString("hive.database"))

    import sparkSession.implicits._

    val visitDF: DataFrame = sparkSession.sql(sql.toString())
    visitDF.show(100)
    visitDF.as[UserVisitAction].rdd
  }

}
