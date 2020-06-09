package com.fashion.spark.streaming.domain

/**
  * 这里使用rowKey 是 day+ courseid的格式
  * @param day_course
  * @param click_count
  */
case class ClickCount(day_course:String, click_count:Long)
