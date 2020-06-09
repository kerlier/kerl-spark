package com.fashion.spark.streaming.util

object HtmlUtil {

  def getCourseId(url:String )  ={
    var courseId = 0
    //"GET /class/128.html HTTP/1.1"
    if(url.startsWith("/class")){
       val courseHtml = url.split("/")(2)
      courseId = courseHtml.substring(0, courseHtml.lastIndexOf(".")).toInt
    }

    courseId
  }

  def main(args: Array[String]): Unit = {
    val url = "/class/128.html"

    val subUrl = url.split("/")

    subUrl.foreach(println)

    println(getCourseId(url))
  }

}
