package com.shao.spark.sparkSql

import com.shao.spark.sparkSql.AppSource.serviceLogic
import org.apache.spark.sql.SparkSession

object TestJsonHanzi {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().appName("BugFlyText").master("local[5]").getOrCreate()
    val dataFrame = sparkSession.read.format("json").load("file:///C:/Users/10703/Desktop/test(1).json")
    dataFrame.show(false)
   // val df0=serviceLogic(dataFrame,sparkSession)
    //df0.show()
  }
}
