package com.gdj.bigdata.flink.controller

import com.gdj.bigdata.flink.common.TController
import com.gdj.bigdata.flink.service.HotItemAnalysesService

/**
  * 热门商品分析控制器
  */
class HotItemAnalysesController extends TController{

    private val hotItemAnalysesService = new HotItemAnalysesService

    /**
      * 执行
      */
    override def execute(): Unit = {
        val result = hotItemAnalysesService.analyses()
        result.print()
    }
}
