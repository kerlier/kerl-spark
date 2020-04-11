package com.fashion.spark.scala

import java.util.Scanner

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 创建scalaSparkContext
  */

class ScalaSparkContext(appName:String="spark"){
  private val sc: SparkContext = getSparkContext()

  def getSparkContext() ={
    val config = new SparkConf().setAppName(appName)
    config.setMaster("local")
    val sc = new SparkContext(config)
    sc // scala没有return ,返回的参数可以在最后写
  }

  def stop()={
    println("按回车键结束")
    val in = new Scanner(System.in)
    in.nextLine()
    sc.stop()// 将sparkContext停掉
  }

}

object ScalaSparkContext {

  def main(args: Array[String]): Unit = {

    val sparkContext = new ScalaSparkContext()
    val sc = sparkContext.sc

    // _表示的是变量
    val fileInput = sc.textFile("E:\\git\\spark\\test.txt").map(_.toLowerCase())

    fileInput
      .flatMap(line=> line.split("."))
      .map(word=>(word,1))
      .reduceByKey((count1,count2)=> count1+count2)
      .saveAsTextFile("../a.txt") //如果这个文件已经存在，会直接报错


    val readInput = sc.textFile("../a.txt").collect()

    println(readInput.mkString(","))

    sparkContext.stop()

  }
}
