package com.shao.spark.project.dao

import com.shao.spark.project.domain.{CourseClickCount, Source}
import com.shao.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  *实战课程访问量数据访问层
  */
object AppSourceDAO {
  val nameSpace = "BugFly"
  val tableName = "BugFly:source"
  val cf = "info" //列簇
  val newUser = "newUser"
  val period = "period"
  val online = "online"
  val flow = "flow"

  /**
    * 保存数据到hbase
    *
    * @param list Source集合
    * @return
    */
  def save(list: ListBuffer[Source]): Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(online),
        ele.online)
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(newUser),
        ele.newUser)
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(flow),
        ele.flow)
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(period),
        ele.period)
    }
  }
  def saverr(list: ListBuffer[Source]): Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(online),
        ele.online)
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(newUser),
        ele.newUser)
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(flow),
        ele.flow)
      table.incrementColumnValue(Bytes.toBytes(ele.key),
        Bytes.toBytes(cf),
        Bytes.toBytes(period),
        ele.period)
    }
  }

}
