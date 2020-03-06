package com.gdj.bigdata.flink.application

import com.gdj.bigdata.flink.common.TApplication
import com.gdj.bigdata.flink.controller.HotItemAnalysesController

/**
  * 热门商品统计应用
  */
object HotItemAnalysesApplication extends App with TApplication {

    // 启动应用程序
    start {
        // Flink获取命令行参数
        //ParameterTool.fromArgs(args).get("host")

        // 执行控制器
        val controller = new HotItemAnalysesController
        controller.execute()
    }

}
