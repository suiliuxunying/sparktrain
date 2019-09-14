package com.shao.spark.project.domain

/**
  *  清洗后的点击日志（spark中类似javabean的可以定义成“case class”）
  * @param ip 日志访问的IP地址
  * @param time 日志访问的时间
  * @param courseId 日志访问的实战课程编号
  * @param statusCode 日志访问的状态
  * @param referer 日志访问的来源
  */
case class ClickLog (ip:String, time:String,courseId:Int,statusCode:Int,referer:String){

}
