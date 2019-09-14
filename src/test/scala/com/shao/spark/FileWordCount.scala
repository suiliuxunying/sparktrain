package com.shao.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark Streaming 处理文件系统（local/hdfs）的数据
  */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    //这里不用“local[2]”
    val sparkConf=new SparkConf().setAppName("FileWordCount").setMaster("local")
    val ssc =new StreamingContext(sparkConf,Seconds(5))
    val lines=ssc.textFileStream("file:///C:/Users/10703/Desktop/data/")
    val result=lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
    //lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
