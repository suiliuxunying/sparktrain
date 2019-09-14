package com.shao.spark.sparkSql

import java.util

import org.apache.spark.sql.SparkSession

object TestSparSqlJson {
  case class Jsona( begintime:Long, data:String, day:util.List[Data], endtime:Long,userId:Long)
  case class Json( begintime:Long,data:String, day:String, endtime:Long, userId:Long)
  case class Data(Package:String, activetime:Int)

   def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local[2]").appName("DataSet").getOrCreate()
    DataFrame1(spark)
    //DataFrame(spark)
    //DataSetTest(spark)
  }

  private def DataFrame1(spark: SparkSession) = {
    val dataFrame = spark.read.format("json").load("file:///C:/Users/10703/Desktop/behavior-json.log")
    dataFrame.show()
    //清理数据的一个low我方法
    import spark.implicits._
   // dataFrame.map(data => data(2).toString().replaceAll("-", "")).show(false)
    //去除json中的json
    dataFrame.select($"userId",$"data.package"(1).as("aa"))//.show()
    dataFrame.select($"userId",dataFrame("data").as("aa"))
    dataFrame.createOrReplaceTempView("app")
    val ss=spark.sql("select userId,explode(data) as aa from app")
    //  +------+--------------------+
    //  |userId|                  aa|
    //  +------+--------------------+
    //  |  2000|[60000,com.browser1]|
    //  |  2000|[120000,com.browser]|
    //  |  2000|[60000,com.browser1]|
    //  |  2000|[120000,com.browser]|
    //  |  2000|[60000,com.browser1]|
    //  |  2000|[120000,com.browser]|
    //  |  2000|[60000,com.browser1]|
    //  |  2000|[120000,com.browser]|
    //  +------+--------------------+
    ss.select($"aa.activetime",$"aa.package").show()
    /*|activetime|     package|
      +----------+------------+
      |     60000|com.browser1|
      |    120000| com.browser|
      |     60000|com.browser1|
      |    120000| com.browser|
      |     60000|com.browser1|
      |    120000| com.browser|
      |     60000|com.browser1|
      |    120000| com.browser|
      +----------+------------+*/
    val df=dataFrame.map(data =>
      Json(
        data(0).toString.toLong,
        data(1).toString,
        data(2).toString,
        data(3).toString().replaceAll("-", "").toLong,
        data(4).toString.toLong
      )).select($"userId",$"data")//.createOrReplaceTempView("app")
    //spark.sql("select userId,day,data.Package,data.activetime from app").show()
  }
  private def DataSetTest(spark: SparkSession): Unit = {
    val path="file:///C:/Users/10703/Desktop/behavior-json.json"
    //val peopleDS = spark.read.json(path).as[Data]
    import spark.implicits._
    //1:
    val caseClassDS  = Seq(Data("Andy", 32)).toDS()
    caseClassDS .show(false)
    //2：
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    primitiveDS.show()
    //3：DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val peopleDS1 = spark.read.json(path).as[Jsona]
    peopleDS1.show()
  }
  private def DataFrame(spark: SparkSession) = {
    val dataFrame = spark.read.format("json").load("file:///C:/Users/10703/Desktop/behavior-json.log")
    dataFrame.show()
    //清理数据的一个low我方法
    import spark.implicits._
    dataFrame.rdd.map(data => data(2).toString().replaceAll("-", "")).toDF().show(false)
    //可以去取出一列
    val row = dataFrame.select(dataFrame.col("day"))
    row.show()
    val arry = row.collect()
    val list = row.collectAsList()
    print(list)
    // split(" ")
    //print(arry.flatMap().sp)
  }
}
//获取json中的json
//val df = sqlContext.read.json("hdfs://master:9000/test/people_Array.json")
//df.show()
//df.printSchema()
//val dfScore = df.select(df("name"),explode(df("myScore"))).toDF("name","myScore")
//val dfMyScore = dfScore.select("name","myScore.score1", "myScore.score2")
//dfScore.show()