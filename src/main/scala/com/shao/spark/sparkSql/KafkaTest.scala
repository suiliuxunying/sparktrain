package com.shao.spark.sparkSql

import com.shao.spark.{Record, SparkSessionSingleton}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object KafkaTest {
  def main(args: Array[String]): Unit = {
   // dataset(args)
    //dataFrame(args)
    rdd(args)
  }
  private def rdd(args: Array[String]) = {
    if(args.length != 2){
      System.err.println( //10.30.211.233:9092 streaming_topic
        "Usage:KafkaDirectWordCount <brokers> " +
          "<topics>")
      System.exit(1)
    }
    val Array(brokers,topics)=args
    val sparkConf=new SparkConf().setAppName("KafkaDirectWordCountSql").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(1))
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
        BugFlyText.serviceLogic(sparkSession, df)
      }}
    //.map((_,1)).reduceByKey(_+_).print()//(aa,1)(ss,2)(dd,1)(ff,1)
    ssc.start()
    ssc.awaitTermination()
  }
  private def dataFrame(args: Array[String]) = {
    def main(args: Array[String]) {
      if (args.length < 2) {
        System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
        System.exit(1)
      }

      val host = args(0)
      val port = args(1).toInt

      val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

      import spark.implicits._

      // Create DataFrame representing the stream of input lines from connection to host:port
      val lines = spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()

      BugFlyText.serviceLogic(spark, lines)
    }
  }

  private def dataset(args: Array[String]) = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics>")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics) = args
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      //.toDF()

   // BugFlyText.serviceLogic(spark, lines)
  }
}
