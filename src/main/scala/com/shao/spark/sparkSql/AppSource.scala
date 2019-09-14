package com.shao.spark.sparkSql

import com.shao.spark.project.dao.{AppSourceDAO, CourseClickCountDAO}
import com.shao.spark.project.domain.CourseClickCount
import com.shao.spark.project.utils.DateUtils
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object AppSource {

  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().appName("BugFlyText").master("local[5]").getOrCreate()
    val dataFrame = sparkSession.read.format("json").load("file:///C:/Users/10703/Desktop/test (1).json")
    dataFrame.show(false)
    val df0=serviceLogic(dataFrame,sparkSession)
    df0.show()
//    df0.rdd.foreachPartition(partitionRecordes=>{
//        val list=new ListBuffer[CourseClickCount]
//
//        partitionRecordes.foreach(pair=>{
//          list.append(CourseClickCount(pair._1,pair._2))
//        })
//
//        CourseClickCountDAO.save(list)
//      })
//    })
//      df0.foreachRDD(rdd=>{
//        rdd.foreachPartition(partitionRecordes=>{
//          val list=new ListBuffer[CourseClickCount]
//
//          partitionRecordes.foreach(pair=>{
//            list.append(CourseClickCount(pair._1,pair._2))
//          })
//
//          CourseClickCountDAO.save(list)
//        })
//      }
//    AppSourceDAO.save(df0.rdd)
    val df1=readHbase(sparkSession)
   print('1')
   df1.show()
    val df2=accumulation(sparkSession,df0,df1)
    print('2')
    df2.show()
    df2.printSchema()
    writeHbase(df2,sparkSession)
    writeHbase(df2,sparkSession)
  }
  /*
  数据清洗
   */
  def serviceLogic(dataFrame:DataFrame,sparkSession: SparkSession): DataFrame ={
    dataFrame.show()
    dataFrame.printSchema()
    import sparkSession.implicits._
    dataFrame.createOrReplaceTempView("App")
    val dataT=sparkSession.sql("select userId ,userId, time ,explode(data) as dataChild from App")
      .select($"userId",$"time",$"userId",$"dataChild.isNewUser".as("NewUser"),$"dataChild.package_id".as("package_id"),$"dataChild.check_times".as("check"),$"dataChild.flows".as("flows"))
  /*  "col0":{"cf":"rowkey", "col":"key", "type":"string"},
    "col1":{"cf":"info", "col":"period", "type":"long"},
    "col2":{"cf":"info", "col":"newUser", "type":"long"},
    "col3":{"cf":"info", "col":"online", "type":"long"},
    "col4":{"cf":"info", "col":"flow", "type":"long"}*/
    dataT.show(false)
    val df=dataT.map(data =>
      ClearData(
        DateUtils.DateFormat(data(1).toString)+"_"+data(4).toString,
        1,
        data(3).toString.toLong,
        1,
        if(data(6)!=null) data(6).toString.toLong else 0
    )).toDF()
    df.show()
    df
      //val df1=df
      //accumulation(sparkSession,df1,df)
}
  /*
  HBase读
   */
  private def readHbase(spark: SparkSession): DataFrame = {

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->testCatalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }
  /*
  HBase写
   */
  private def writeHbase(dataFrame:DataFrame,spark: SparkSession) = {
    dataFrame.write.mode("Append")
      // .mode(SaveMode.Overwrite)
      .options(Map(HBaseTableCatalog.tableCatalog -> testCatalog))
      .format("org.apache.spark.sql.execution.datasources.hbase").saveAsTable("source")
    //.save()
  }
/*
累加
 */
  private def accumulation(sparkSession: SparkSession,df: DataFrame, df1: DataFrame): DataFrame = {
    import sparkSession.implicits._
    val df2=df.join(df1,df1.col("col0")===df.col("col0"),"full")
    df2.show()
      df2.map(data => ClearData(
      if(data(0)!=null) data(0).toString else data(5).toString,
        if(data(1)!=null&& data(6)!=null) data(1).toString.toLong.+(data(6).toString.toLong) else {if(data(1)!=null) data(1).toString.toLong else data(6).toString.toLong},
        if(data(2)!=null&& data(7)!=null) data(2).toString.toLong.+(data(7).toString.toLong) else {if(data(2)!=null) data(2).toString.toLong else data(7).toString.toLong},
        if(data(3)!=null&& data(8)!=null) data(3).toString.toLong.+(data(8).toString.toLong) else {if(data(3)!=null) data(3).toString.toLong else data(8).toString.toLong},
        if(data(4)!=null&& data(9)!=null) data(4).toString.toLong.+(data(9).toString.toLong) else {if(data(4)!=null) data(4).toString.toLong else data(9).toString.toLong}
      )).toDF()
  }
  /*
  数据输出类型
   */
  case class ClearData(col0: String,
                       col1: Long,
                       col2: Long,
                       col3: Long,
                       col4: Long)
  /*
  HBase表结构
   */
  def testCatalog =s"""{
                      |"table":{"namespace":"BugFly", "name":"source"},
                      |"rowkey":"key",
                      |"columns":{
                      |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                      |"col1":{"cf":"info", "col":"period", "type":"long"},
                      |"col2":{"cf":"info", "col":"newUser", "type":"long"},
                      |"col3":{"cf":"info", "col":"online", "type":"long"},
                      |"col4":{"cf":"info", "col":"flow", "type":"long"}
                      |}
                      |}""".stripMargin
}
