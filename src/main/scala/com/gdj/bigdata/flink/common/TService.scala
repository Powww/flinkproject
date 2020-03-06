package com.gdj.bigdata.flink.common

import com.gdj.bigdata.flink.bean.UserBehavior
import org.apache.flink.streaming.api.scala._

/**
  * 通用服务特质
  */
trait TService {
    // 分析
    def getDao():TDao

    // 分析
    def analyses():Any

    /**
     * 获取用户行为的封装数据
     */
    protected def getUserBehaviorDatas() = {
        val dataDS: DataStream[String] = getDao.readTextFile("input/UserBehavior.csv")
        // TODO 1. 将原始数据进行封装对象，方便后续使用
        val userBehaviorDS: DataStream[UserBehavior] = dataDS.map(
            data => {
                val datas = data.split(",")
                UserBehavior(
                    datas(0).toLong,
                    datas(1).toLong,
                    datas(2).toLong,
                    datas(3),
                    datas(4).toLong
                )
            }
        )
        userBehaviorDS
    }
}
