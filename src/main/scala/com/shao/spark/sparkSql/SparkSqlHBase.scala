package com.shao.spark.sparkSql

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}
object SparkSqlHBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("normal").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    //writeHbase(spark)
    val testData = (0 to 11).map { i => TestHBaseRecord(i, "Test") }
    val df: DataFrame = spark.createDataFrame(testData)
    df.show()
    testWriteHbase(df,spark)
    //testReadHbase(spark)
  }

  private def testReadHbase(spark: SparkSession): DataFrame = {

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->testCatalog))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
  }
  private def testWriteHbase(dataFrame:DataFrame,spark: SparkSession) = {
    dataFrame.write.mode("Overwrite")
      // .mode(SaveMode.Overwrite)
      .options(Map(HBaseTableCatalog.tableCatalog -> testCatalog))
      .format("org.apache.spark.sql.execution.datasources.hbase").saveAsTable("source")
      //.save()
  }

  private def writeHbase(spark: SparkSession) = {
    val data = (0 to 10).map { i => HBaseRecord(i, "source") }
    val df: DataFrame = spark.createDataFrame(data)
    df.show()
    //    spark.(data).toDF.write.options(
    //      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .save()
    val a = df.write
      // .mode(SaveMode.Overwrite)
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
    try {
    //  a.save()
    } catch {
      case e: Exception => {
        println("*****BugFly")
      }
    }
  }

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

  def catalog = s"""{
                   |"table":{"namespace":"rec", "name":"user_rec"},
                   |"rowkey":"key",
                   |"columns":{
                   |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"col1":{"cf":"t", "col":"col1", "type":"boolean"},
                   |"col2":{"cf":"t", "col":"col2", "type":"double"},
                   |"col3":{"cf":"t", "col":"col3", "type":"float"},
                   |"col4":{"cf":"t", "col":"col4", "type":"int"},
                   |"col5":{"cf":"t", "col":"col5", "type":"bigint"},
                   |"col6":{"cf":"t", "col":"col6", "type":"smallint"},
                   |"col7":{"cf":"t", "col":"col7", "type":"string"},
                   |"col8":{"cf":"t", "col":"col8", "type":"tinyint"}
                   |}
                   |}""".stripMargin

}
case class TestHBaseRecord(
                            col0: String,
                            col1: Long,
                            col2: Long,
                            col3: Long,
                            col4: Long
                          )
case class HBaseRecord(
                        col0: String,
                        col1: Boolean,
                        col2: Double,
                        col3: Float,
                        col4: Int,
                        col5: Long,
                        col6: Short,
                        col7: String,
                        col8: Byte)

  object HBaseRecord
  {
    def apply(i: Int, t: String): HBaseRecord = {
      val s = s"""row${"%03d".format(i)}"""
      HBaseRecord(s,
        i % 2 == 0,
        i.toDouble,
        i.toFloat,
        i,
        i.toLong,
        i.toShort,
        s"String$i: $t",
        i.toByte)
    }
}
object TestHBaseRecord
{
  def apply(i: Int, t: String):TestHBaseRecord={
    val s = s"""row${"%03d".format(i)}"""
    TestHBaseRecord(s,
      1,
      1,
      1,
      1
    )
  }
}
