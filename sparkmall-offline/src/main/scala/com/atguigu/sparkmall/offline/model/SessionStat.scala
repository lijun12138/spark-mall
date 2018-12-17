package com.atguigu.sparkmall.offline.model

/** *
  *
  * @param taskId                           统计批次编号
  * @param contiditions                     本次过滤条件
  * @param session_count                    总session个数
  * @param session_visitLength_le_10s_ratio 小于等于10秒
  * @param session_visitLength_gt_10s_ratio 大于10秒
  * @param session_stepLength_le_5_ratio    步长小于 等于5次
  * @param session_stepLength_gt_5_ratio    大于5次
  */
case class SessionStat(var taskId: String, var contiditions: String, session_count: Long, session_visitLength_le_10s_ratio: Double, session_visitLength_gt_10s_ratio: Double, session_stepLength_le_5_ratio: Double, session_stepLength_gt_5_ratio: Double) {

}

