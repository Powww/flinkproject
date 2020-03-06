package com.gdj.bigdata.flink.common

import com.gdj.bigdata.flink.util.FlinkStreamEnv


trait TApplication {
    def start( op: => Unit ): Unit = {
        try {
            // 初始化Flink的运行环境
            FlinkStreamEnv.init
            op
            // 执行Flink环境
            FlinkStreamEnv.execute()
        } catch {
            case e => e.printStackTrace()
        } finally {
            FlinkStreamEnv.clear()
        }
    }
}
