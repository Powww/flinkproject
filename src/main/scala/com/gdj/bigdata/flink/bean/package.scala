package com.gdj.bigdata.flink

/**
 * @author gdj
 * @create 2020-03-06-13:50
 *
 */
package object bean {
  /**
   * 用户行为数据
   */
  case class UserBehavior(
                           userId:Long,
                           itemId:Long,
                           categroyId:Long,
                           behavior:String,
                           timestamp:Long)

  /**
   * 热门商品点击
   * @param itemId
   * @param clickCount
   */
  case class HotItemClick(
                           itemId:Long,
                           clickCount:Long,
                           windowEndTime:Long)
}
