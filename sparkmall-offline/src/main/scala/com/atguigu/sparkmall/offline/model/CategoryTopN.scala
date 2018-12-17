package com.atguigu.sparkmall.offline.model

/**
  *
  * @param taskId      任务批次id
  * @param category_id 品类id
  * @param click_count 点击次数
  * @param order_count 下单次数
  * @param pay_count   付款次数
  */
case class CategoryTopN(taskId: String, category_id: String, click_count: Long, order_count: Long, pay_count: Long) {

}
