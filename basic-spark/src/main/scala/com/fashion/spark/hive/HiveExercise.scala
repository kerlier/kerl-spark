package com.fashion.spark.hive

import org.apache.spark.sql.SparkSession

object HiveExercise {

  def main(args: Array[String]): Unit = {

    //创建sparkSession
    val sparkSession = SparkSession.builder().appName("hive")
      .master("local[2]")
      .config("spark.sql.warehouse.dir","E:\\spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    //从sparkSession获取sparkContext
    val sc = sparkSession.sparkContext

    sc.setLogLevel("WARN")

    sparkSession.sql("create table if not exists Person(id int, name string, age int) row format delimited fields" +
      " terminated by ' '")
//    sparkSession.sql("LOAD DATA LOCAL INPATH 'F:\\person.txt' into table Person")

    sparkSession.sql("select * from Person").show()

    sparkSession.stop()
  }
}
