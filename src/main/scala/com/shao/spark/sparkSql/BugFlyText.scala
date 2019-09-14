package com.shao.spark.sparkSql

import com.shao.spark.project.dao.AppSourceDAO
import com.shao.spark.project.domain.Source
import com.shao.spark.project.utils.DateUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object BugFlyText {
  var status=10
  case class ClearRealTimeData(time_app:String,flow:Long,activeTime:Long)

  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().appName("BugFlyText").master("local[2]").getOrCreate()
    val dataFrame = sparkSession.read.format("json").load("file:///C:/Users/10703/Desktop/test(1).json" )
    dataFrame.show()
    print(dataFrame.col("userId"))
    serviceLogic(sparkSession,dataFrame)
  }
  def serviceLogic(sparkSession: SparkSession,dataFrame:DataFrame): Unit ={
      import sparkSession.implicits._
      dataFrame.createOrReplaceTempView("App")
      val dataT=sparkSession.sql("select userId ,ActiveApp, time ,explode(data) as dataChild from App")
        .select($"userId",$"time",$"ActiveApp".as("ActiveApp"),$"dataChild.isNewUser".as("newUser"),$"dataChild.package_id".as("package_id"),$"dataChild.check_times".as("check"),$"dataChild.flows".as("flows"),$"dataChild.package_name".as("AppName"))
      dataT.show(false)
      dataT.printSchema()
      appData(dataT,sparkSession)

      //实时表（dataT必须有“time”“flows”“package_id”）
      // realTime(dataT,sparkSession)
}
  def appData(dataT: DataFrame,sparkSession: SparkSession)={
      import sparkSession.implicits._
      //App表的数据
      val appdf=dataT.select($"time",$"package_id",$"flows",$"newUser")
      val df=appdf.map(data =>
        Source(
          DateUtils.DateFormat(data(0).toString)+"_"+data(1).toString,
          1,
          1,
          data(3).toString.toLong,
          data(2).toString.toLong
        ))
        .groupBy($"key").agg(
      "period" -> "sum", "flow" -> "sum", "newUser" -> "sum", "onLine" -> "sum")

    var  appDfSum=Seq(Source("1",0,0,0,0))toDF()
    appDfSum= appDataAccumulation(sparkSession,appDfSum,df)
    appDfSum.show(100,false)
    if(status==10){//appDfSum.show();
      appDfSum.rdd.foreachPartition(partitionRecordes=>{
        val list=new ListBuffer[Source]
        partitionRecordes.foreach(pair => {
          list.append(Source(pair.get(0).toString,
            pair.get(1).toString.toLong,
            pair.get(2).toString.toLong,
            pair.get(3).toString.toLong,
            pair.get(4).toString.toLong))
        })
       AppSourceDAO.save(list)
      });
      status=0}
    //记录数据批次
    status=status+1
  }
  //realTime表数据 rowKey:“time_AppID”
  private def realTime(dataT: DataFrame,sparkSession:SparkSession) = {
    import sparkSession.implicits._
    dataT.show()
    val df =dataT.select($"time",$"package_id",$"flows")
    df.map(data =>
      ClearRealTimeData(
        DateUtils.DateTimeFormat(data(0).toString) + "_" + data(1).toString,
        data(2).toString.toLong,
        1
      )).toDF().show()
    dataT.foreach(row => (
      print(row)
      ))

  }

  /*
  App表累加
   */
  private def appDataAccumulation(sparkSession: SparkSession,df: DataFrame, df1: DataFrame): DataFrame = {
    import sparkSession.implicits._

    if(df.first()(0)=="1"){
      df1
    }else{
    val df2=df.join(df1,df.col("key")===df1.col("key"),"full")
    df2.show()
    df2.map(data => Source(
      if(data(0)!=null) data(0).toString else data(5).toString,
      if(data(1)!=null&& data(6)!=null) data(1).toString.toLong.+(data(6).toString.toLong) else {if(data(1)!=null) data(1).toString.toLong else data(6).toString.toLong},
      if(data(2)!=null&& data(7)!=null) data(2).toString.toLong.+(data(7).toString.toLong) else {if(data(2)!=null) data(2).toString.toLong else data(7).toString.toLong},
      if(data(3)!=null&& data(8)!=null) data(3).toString.toLong.+(data(8).toString.toLong) else {if(data(3)!=null) data(3).toString.toLong else data(8).toString.toLong},
      if(data(4)!=null&& data(9)!=null) data(4).toString.toLong.+(data(9).toString.toLong) else {if(data(4)!=null) data(4).toString.toLong else data(9).toString.toLong}
    )).toDF()
    }
  }
}
