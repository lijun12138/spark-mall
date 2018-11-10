package com.atguigu.sparkmall.offline.model

/**
  *
  *
  * @author lijun
  * @date 2018/11/10
  */
class SessionInfo {

  case class SessionInfo(sessionId: String, startTime: String, searchKeywords: String, clickCategoryIds: String, stepLength: Long, visitLength: Long) {

  }

}
