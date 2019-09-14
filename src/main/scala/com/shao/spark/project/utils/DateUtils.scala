package com.shao.spark.project.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.joda.time.DateTime
/**
  * 日期时间工具类
  */
object DateUtils {
  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyMMddHHmmss")

  def getTime(time: String)={
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }
  //输入：“yyyy-MM-dd HH:mm:ss” 返回“yyyMMddHHmmss”
  def parseToMinute(time:String)={
    TARGE_FORMAT.format(new Date(getTime(time)))
  }
  /**
    * 接受一个时间戳的参数，返回日期
    */
  def DateFormat(time:String):String={
    var sdf:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var date:String = sdf.format(new Date((time.toLong)))
    date
  }
  def DateTimeFormat(time:String):String={
    var sdf:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    var date:String = sdf.format(new Date((time.toLong)))
    date
  }
  def main(args: Array[String]): Unit = {
    println(DateTimeFormat("1522246957868"))
    //println(parseToMinute("2018-03-12 21:09:01"))

  }
}