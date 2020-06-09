package com.fashion.spark.streaming.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {

  val BEFORE_FORMAT =  FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT =  FastDateFormat.getInstance("yyyyMMddHHmmss")

  // 获取时间戳
  def getTime(time:String) = {
    BEFORE_FORMAT.parse(time).getTime()
  }

  def parseToMinute(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2020-06-07 00:34:01"))
  }
}
