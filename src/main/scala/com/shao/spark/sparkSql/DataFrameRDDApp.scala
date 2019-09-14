package com.shao.spark.sparkSql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameRDDApp {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    //inferReflect ion(spark)
    program(spark)
    spark.stop()
  }
  private def program(session: SparkSession): Unit ={
    //得到rdd
    val rdd = session.sparkContext.textFile("file:///D:\\download\\BaiduNetdiskDownload\\muke\\10小时入门大数据\\源码\\b1qv7n\\sparktrain\\data/info.log")
    val infoRDD=rdd.map(_.split("\t")).map(line=> Row(line(0)toInt,line(1).toInt,line(2)))
    val structType=StructType(Array(StructField("id",IntegerType,false),
      StructField("age",IntegerType,true),
      StructField("name",StringType,true)
    ))
    val infoDF=session.createDataFrame(infoRDD,structType)
    infoDF.show()
    infoDF.printSchema()
  }
  private def inferReflection(spark: SparkSession) = {
    //RDD==>DataFrame
    //得到rdd
    val rdd = spark.sparkContext.textFile("file:///D:\\download\\BaiduNetdiskDownload\\muke\\10小时入门大数据\\源码\\b1qv7n\\sparktrain\\data/info.log")
    //为toDF方法导入隐式转换
    import spark.implicits._
    val dataFrame = rdd.map(_.split("\t")).map(line => Info(line(0).toInt, line(1).toInt, line(2))).toDF()
    dataFrame.show()
    dataFrame.filter(dataFrame.col("age") > 30).show()
    //SQL方式
    //将DataFrame注册为SQL临时视图                                                                                                                                                                                                                                                                              为一张表
    dataFrame.createOrReplaceTempView("infos")
    spark.sql("select * from infos").show()
    spark.sql("select * from infos where age > 30").show()
  }

  //类似java bean
  case class Info(id:Int,age:Int,name:String)
}
