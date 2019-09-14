package com.shao.spark.sparkSql

import com.shao.spark.{ SparkSessionSingleton}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object TextKafkaSqlJson {
  def main(args: Array[String]):  Unit = {
    if(args.length != 2){
      System.err.println( //10.30.211.233:9092 streaming_topic
        "Usage:KafkaDirectWordCount <brokers> " +
          "<topics>")
      System.exit(1)
    }
    val Array(brokers,topics)=args
    val sparkConf=new SparkConf().setAppName("KafkaDirectWordCountSql").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //todo..如何使用spark streaming整合kafka
    val topicSet=topics.split(",").toSet
    val kafkaParams=Map[String,String]("metadata.broker.list" ->brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    kafkaStream.foreachRDD { rdd =>
      //extracting the values only
      if(rdd.isEmpty){
        print("null")
      }else{

      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      val df = sparkSession.read.json(rdd.map(x => x._2))
      df.show()
        print(df.takeAsList(2))
        print(df.collect().toString)
        print(df.collectAsList().toString)
     // print(df.show()==null)

        df.printSchema()
        val a=df.select("day")
        print(a)
        df.select(df.col("userId"),df.col("day")).show()
    }}
    //.map((_,1)).reduceByKey(_+_).print()//(aa,1)(ss,2)(dd,1)(ff,1)
    ssc.start()
    ssc.awaitTermination()
  }
}
