package com.shao.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark Streaming 处理文件系统（local/hdfs）的数据
  */
object TransformAPP {
  def main(args: Array[String]): Unit = {
    //这里用“local[2]”
    val sparkConf=new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val ssc =new StreamingContext(sparkConf,Seconds(5))
    /**
      * 构建黑名单
      */
    val blacks=List("zs","ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))

    /**
      * 输入信息：
        20180808,zs
        20180908,ls
        20180708,ww
      */
    val lines=ssc.socketTextStream("192.168.0.140",6789)//数据输入，每次一行 这里是从虚拟机输入

    val clickLog = lines.map(x => (x.split(",")(1),x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })
    clickLog.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
