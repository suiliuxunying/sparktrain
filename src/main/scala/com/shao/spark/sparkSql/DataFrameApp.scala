package com.shao.spark.sparkSql

import org.apache.spark.sql.SparkSession

/**
  * dataFrame api的基本操作
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()
    //将json转换成一个dataFrame
    val dataFrame=spark.read.format("json").load("file:///C:/Users/10703/Desktop/behavior-json.log")
    //输出dataFrame对应的schema信息
    dataFrame.printSchema()
    //输出dataframe（默认20条）
   // dataFrame.show()
    //查询某个列的说有信息：sql：select name from table
    //dataFrame.select("userId").show()
    //另一种使用查询多列 支持计算
    //dataFrame.select(dataFrame.col("userId"),dataFrame.col("data"),dataFrame.col("endtime")+11).show(20,false)
    //别名
    //dataFrame.select(dataFrame.col("userId"),dataFrame.col("data"),(dataFrame.col("endtime")+11).as("endtime1")).show(20,false)
    //过滤
    //dataFrame.filter(dataFrame.col("endtime")>1).show()
    //根据某列进行聚合在进行分组
//    dataFrame.groupBy(dataFrame.col("endtime")).count().show()

    dataFrame.first()//显示第一条记录
    //dataFrame.head(2)//显示第二条

    spark.stop()


  }
}
