package com.shao.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 整合 kafka receiver实战
  */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    if(args!=4){
      System.err.println(
        "Usage:kafkaReceiverWordConunt <zkQuorum> <group> " +
          "<topics> <numThreads>")
    }
    val Array(zkQuorum,group,topics,numThreads)=args
    val sparkConf=new SparkConf()//.setAppName("KafkaReceiverWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    //todo..如何使用spark streaming整合kafka
    val topicMap=topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStream =KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    kafkaStream.print()
    //要从2开始取
    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
