package com.fashion.spark.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object ReadMysqlExercise {

  def main(args: Array[String]): Unit = {
     //1 创建sparkSession
      val sparkSession = SparkSession.builder().appName("readMysql")
      .master("local[2]")
      .getOrCreate()

       val properties = new Properties()
       properties.setProperty("user","root")
       properties.setProperty("password","root")

       val mysqlDataFrame = sparkSession.read.jdbc("jdbc:mysql://192.168.5.128:3306/spark","person",properties)

       mysqlDataFrame.show()

       sparkSession.stop()
  }
}
