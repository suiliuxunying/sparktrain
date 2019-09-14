package com.shao.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import _root_.kafka.serializer.StringDecoder //why??
/**
  * spark streaming 整合 kafka receiver实战
  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      System.err.println(
        "Usage:KafkaDirectWordCount <brokers> " +
          "<topics>")
      System.exit(1)
    }
    val Array(brokers,topics)=args
    val sparkConf=new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    //todo..如何使用spark streaming整合kafka
    val topicSet=topics.split(",").toSet
    val kafkaParams=Map[String,String]("metadata.broker.list" ->brokers)
    val kafkaStream = {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    }
    kafkaStream.print() //(null,ss dd ff aa ss)
    //要从2开始取
    kafkaStream.map(_._2)
      .flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_+_).print()//(aa,1)(ss,2)(dd,1)(ff,1)
    ssc.start()
    ssc.awaitTermination()
  }
}
