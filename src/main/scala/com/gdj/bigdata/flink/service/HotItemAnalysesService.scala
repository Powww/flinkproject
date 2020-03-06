package com.gdj.bigdata.flink.service

import com.gdj.bigdata.flink.common.TService
import com.gdj.bigdata.flink.dao.HotItemAnalysesDao
import com.gdj.bigdata.flink.common.TService
import com.gdj.bigdata.flink.dao.HotItemAnalysesDao
import org.apache.flink.streaming.api.scala._

/**
  * 热门商品分析
  */
class HotItemAnalysesService extends TService {

    private val hotItemAnalysesDao = new HotItemAnalysesDao();

    /**
      * 数据分析
      */
    override def analyses() = {
        val dataDS: DataStream[String] = hotItemAnalysesDao.readTextFile

        val resultDS: DataStream[(String, Int)] = dataDS
                .flatMap(_.split(" "))
                .map((_, 1))
                .keyBy(_._1)
                .sum(1)

        resultDS
    }
}
