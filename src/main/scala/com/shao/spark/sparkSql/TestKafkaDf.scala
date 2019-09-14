package com.shao.spark.sparkSql

import com.shao.spark.{Record, SparkSessionSingleton}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
object TestKafkaDf {
 def main(args: Array[String]): Unit = {
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
    val kafkaStream = {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    }
    //kafkaStream.print() //(null,ss dd ff aa ss)
    //要从2开始取
    val words = kafkaStream.map(_._2)
      .flatMap(_.split(" "))
    words.print()
    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SparkSession
      // Get the singleton s of SparkSession
      //val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._
      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      wordsDataFrame.printSchema()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      //println(s"========= $time =========")
      wordCountsDataFrame.show()
    }
      //.map((_,1)).reduceByKey(_+_).print()//(aa,1)(ss,2)(dd,1)(ff,1)
    ssc.start()
    ssc.awaitTermination()
  }
}
