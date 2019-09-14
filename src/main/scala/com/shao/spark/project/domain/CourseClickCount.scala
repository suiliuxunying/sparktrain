package com.shao.spark.project.domain

/**
  * 实战课程点击数实体类
  * @param day_course 对应hbase的row_key，20180312_1
  * @param click_count 20180312_1对应的访问量
  */
case class CourseClickCount (day_course:String,click_count:Long)
