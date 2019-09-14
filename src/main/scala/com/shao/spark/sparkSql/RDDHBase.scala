package com.shao.spark.sparkSql

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

object RDDHBase {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().appName("BugFlyText").master("local[5]").getOrCreate()
    val dataFrame = sparkSession.read.format("json").load("file:///C:/Users/10703/Desktop/bugfly1.log")
    val rdd=dataFrame.rdd
    rdd.foreachPartition { records =>
      val config = HBaseConfiguration.create
      config.set("hbase.zookeeper.property.clientPort", "2181")
      config.set("hbase.zookeeper.quorum", "hadoop000")
      val connection = ConnectionFactory.createConnection(config)
      val table = connection.getTable(TableName.valueOf("BugFly:source"))

      // 举个例子而已，真实的代码根据records来
      val list = new java.util.ArrayList[Put]
      for(i <- 0 until 10){
        val put = new Put(Bytes.toBytes(i.toString))

        list.add(put)
      }
      // 批量提交
      table.put(list)
      // 分区数据写入HBase后关闭连接
      table.close()
    }
  }
}
