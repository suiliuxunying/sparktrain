package com.shao.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming 完成有状态统计
  */
object ForeachEDDApp {
  //Scala的main函数
  def main(args: Array[String]): Unit = {
    //在本地运行，不需要Hadoop环境
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    //如果使用了Stateful的算子，必须要设置checkpoint
    //生产环境中，建议大家将checkpoint设置在HDFS的某个文件夹中
    ssc.checkpoint(".")
    val lines=ssc.socketTextStream("10.30.211.233",6789)//数据输入，每次一行 这里是从虚拟机输入
    //将数据用空格分开 并且为每个值加上一个“1” 再将相同的相加
    val result=lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()//处理结果打印在控制台
    //处理结果存入mysql数据库
    result.foreachRDD(rdd=>{
      //会报序列化异常（connection不能被序列化）
//      val connection = createConnection()  // executed at the driver
//      rdd.foreach { record =>
//        val sql="insert into wordcount(word,wordcount) values('"+record._1+"',"+record._2+")"
//        connection.createStatement().execute(sql) // executed at the worker
//      }
      //官方推介
      rdd.foreachPartition { partitionOfRecords =>
        //if(partitionOfRecords.size>0){
          val connection = createConnection()
          partitionOfRecords.foreach(record =>
          {
            val sql="insert into wordcount(word,wordcount) values('"+record._1+"',"+record._2+")"
            connection.createStatement().execute(sql)
          })
          connection.close()
        }
     // }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取mysql的连接
    * @return
    */
  def createConnection() ={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_wordcount","root","")
  }
}

