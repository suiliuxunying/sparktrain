package com.shao.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark streaming 只能和Flume
  */
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      System.err.println("Usage:FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }
    //args传入的(传入的肯定是String类型)
    val Array(hostname,port)= args
    //打包生产上运行不需要
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    //todo...如何让使用spark streaming整合flume
     val flumeStream=FlumeUtils.createStream(ssc,hostname,port.toInt)
    //拿到内容（flume数据分为hand和body 先拿到body再转化成字节数组再修整(去掉空格)的到数据）
    flumeStream.map(x=>new  String(x.event.getBody.array()).trim )
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
