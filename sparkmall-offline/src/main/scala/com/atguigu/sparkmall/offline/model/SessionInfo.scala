package com.atguigu.sparkmall.offline.model


/** *
  *
  * @param taskId           统计批次编号
  * @param sessionId        sessionId
  * @param startTime        访问开始时间
  * @param searchKeywords   搜索的单词
  * @param clickCategoryIds 点击的商品类别
  * @param stepLength       访问步长
  * @param visitLength      访问时长
  * @param clickProductIds  点击过的商品
  * @param orderCategoryIds 下过单的商品类别
  * @param orderProductIds  下过单的商品
  * @param payCategoryIds   支付过的品类别
  * @param payProductIds    支付过的商品
  */
case class SessionInfo(var taskId: String, sessionId: String, startTime: String, searchKeywords: String, clickCategoryIds: String, stepLength: Long, visitLength: Long, clickProductIds: String, orderCategoryIds: String, orderProductIds: String, payCategoryIds: String, payProductIds: String, professional: String) {

}

