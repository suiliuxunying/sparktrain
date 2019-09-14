package com.shao.spark.sparkSql

import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration
import org.apache.spark.sql.SparkSession

object DataSet {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local[2]").appName("DataSet").getOrCreate()
    //导入隐式转换
    import spark.implicits._
    val path ="file:///D:\\download\\BaiduNetdiskDownload\\muke\\10小时入门大数据\\源码\\b1qv7n\\sparktrain\\data/info.csv"
    //spark 如何解析csv文件
    val df=spark.read.option("header","true").option("inferSchema","true").csv(path)
    df.show()

    val ds = df.as[Pepole]
    ds.map(line => line.name).show
  }
  case class Pepole(id:Int,name:String,age:Int)
 }
