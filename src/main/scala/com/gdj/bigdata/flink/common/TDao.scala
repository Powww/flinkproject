package com.gdj.bigdata.flink.common

import com.gdj.bigdata.flink.util.FlinkStreamEnv

/**
  * 通用数据访问特质
  */
trait TDao {

    /**
      * 读取文件
      */
    def readTextFile( implicit path:String ) = {
        FlinkStreamEnv.get().readTextFile(path)
    }

    /**
      * 读取Kafka数据
      */
    def readKafka() = {

    }

    /**
      * 读取Socket网络数据
      */
    def readSocket(): Unit = {

    }
}
