package com.shao.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark Streaming 处理 Soket数据
  * 测试 ：nc
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
      //不能用"local[1]"或"local"
    val sparkConf =new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    /**
      * 创建StreamingContext需要两个参数 SparkConf 和 batch interval
      */
    val ssc= new StreamingContext(sparkConf,Seconds(5))
    val lines =ssc.socketTextStream("10.210.105.55",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
