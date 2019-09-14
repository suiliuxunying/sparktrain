package com.shao.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming 完成有状态统计
  */
object StatefulWordCount {
  //Scala的main函数
  def main(args: Array[String]): Unit = {
    //在本地运行，不需要Hadoop环境
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    //如果使用了Stateful的算子，必须要设置checkpoint
    //生产环境中，建议大家将checkpoint设置在HDFS的某个文件夹中
    ssc.checkpoint(".")
    val lines=ssc.socketTextStream("192.168.0.140",6789)//数据输入，每次一行 这里是从虚拟机输入
    //将数据用空格分开 并且为每个值加上一个“1”
    val result=lines.flatMap(_.split(" ")).map((_,1))
    "result:"+result.print()
    //调用函数处理结果
    val state =result.updateStateByKey[Int](updateFunction _)
    state.print()//处理结果打印在控制台
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 把当前的数据加上已有的或者说老的数据一起处理
    * @param currentValues 当前的
    * @param previousValues 老的
    * @return
    */
  def updateFunction(currentValues: Seq[Int], previousValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum  // add the new values with the previous running count to get the new count
    val pre =previousValues.getOrElse(0)
    Some(current + pre)
  }
}
