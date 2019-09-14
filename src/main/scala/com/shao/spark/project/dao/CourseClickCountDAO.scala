package com.shao.spark.project.dao


import com.shao.spark.project.domain.CourseClickCount
import com.shao.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  *实战课程访问量数据访问层
  */
object CourseClickCountDAO {
  val tableName = "course_clickcount"
  val cf ="info" //列簇
  val qualifer = "clink_count"

  /**
    * 保存数据到hbase
    * @param list CourseClickCount集合
    * @return
    */
  def save(list:ListBuffer[CourseClickCount]):Unit= {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for (ele <- list){
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }

  /**
    * 根据row_key查询值
    * @param day_course
    * @return
    */
  def count(day_course:String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    //第一次查询一定没有数据，所以要：
    if (value == null) {
      0l
    } else {
      Bytes.toLong(value)
    }
  }
    //0l//先放一个不报错
    def main(args: Array[String]): Unit = {
      val list=new ListBuffer[CourseClickCount]
      list.append(CourseClickCount("20171111_0",16))
      list.append(CourseClickCount("20171111_1",1))
      list.append(CourseClickCount("20171111_3",100))

      //save(list)
      print(count("20180314_146")+":"+count("20171111_1")+":"+count("20171111_3"))
    }
}
