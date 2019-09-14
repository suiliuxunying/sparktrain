package com.shao.spark.project.spark

import _root_.kafka.serializer.StringDecoder
import com.shao.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.shao.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.shao.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer //why??
/**
  * spark streaming 整合 kafka receiver flume 实现模拟日志获取
  */
object SparkStreamingApp{
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      System.err.println(
        "Usage:KafkaDirectWordCount <brokers> " +
          "<topics>")
      System.exit(1)
    }
    val Array(brokers,topics)=args
    val sparkConf=new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(30))
    //todo..如何使用spark streaming整合kafka
    val topicSet=topics.split(",").toSet
    val kafkaParams=Map[String,String]("metadata.broker.list" ->brokers)
    val kafkaStream = {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    }
     kafkaStream.print()
//测试步骤一，接收数据  要从2开始取/
    // 应该是100一次但是flume到卡夫卡会一次传不够100
    // 80，16，4或者80，20 这样的
    //spark拿数据比较慢，大概要晚20秒
    kafkaStream.map(_._2).count().print()
// 测试步骤二：数据清洗
    //拿到数据(每一行)
    val logs =kafkaStream.map(_._2)
    logs.print()
    //"GET class/146.html HTTP/1.1"====> 146
    val cleanData = logs.map(line=>{
      val infos=line.split("\t")

      //infos(2):GET class/146.html HTTP/1.1
      //split(" ")(1)："class/146.html"
      val url =infos(2).split(" ")(1)
      var courseId = 0//注意“var”

      //拿到实战课程编号
      if(url.startsWith("class")){
        val courseIdHTML=url.split("/")(1)//"146.html" 我的日志少个“/”
        courseId =courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt // 146
        //println(courseId);//测试
      }
      //清洗后存入实体类ClickLog
      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))
    }).filter(clickLog => clickLog.courseId != 0)

    cleanData.print()
//步骤三：统计今天到现在为止的实战课程的访问量
    cleanData.map(x =>{
      //Hbase row_key设计：20171111_88
      (x.time.substring(0,8)+"_"+x.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecordes=>{
        val list=new ListBuffer[CourseClickCount]

        partitionRecordes.foreach(pair=>{
          list.append(CourseClickCount(pair._1,pair._2))
        })

       // CourseClickCountDAO.save(list)
      })
    })
    //步骤四：统计今天到现在为止从收缩引擎过来的实战课程的访问量
    cleanData.map(x =>{
      /**
        * http://www.sogou.com/web?qu..=>
        * http:/www.sogou.com/web?qu..
        */
      val referer=x.referer.replaceAll("//","/")
      val splits=referer.split("/")
      var host=""
      //"_"的话长度为“1”
      if(splits.length>2){
        host =splits(1)
      }
      (host,x.courseId,x.time)//返回的x数组
    }).filter(_._1!="")//如果host=unll就去掉
      .map(x=>{
      (x._3.substring(0,8)+"_"+x._1+"-"+x._2,1)
    }).reduceByKey(_+_).foreachRDD(rdd=> {
      rdd.foreachPartition(partitionRecordes => {
        val list = new ListBuffer[CourseSearchClickCount]
        partitionRecordes.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })
       //CourseSearchClickCountDAO.save(list)
      })
    })
        ssc.start()
        ssc.awaitTermination()

    }
}
