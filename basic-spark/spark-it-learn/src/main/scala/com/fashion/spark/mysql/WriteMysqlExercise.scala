package com.fashion.spark.mysql

import java.util.Properties

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object WriteMysqlExercise {
  val schema = StructType(
    Seq(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)
    )
  )

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("readMysql")
      .master("local[2]")
      .getOrCreate()


    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")

    val sc = sparkSession.sparkContext

    val lineRdd = sc.textFile("E:\\person.txt").map(_.split(" "))

    val personRdd = lineRdd.map(x=>Row(x(0).toInt,x(1),x(2).toInt))

    val personDataFrame = sparkSession.createDataFrame(personRdd,schema)

    //只要是dataFrame，就可以存入到mysql中
    personDataFrame.write.jdbc("jdbc:mysql://192.168.200.150:3306/spark","student",prop)

    //追加表, 使用saveMode
//    personDataFrame.write.mode(SaveMode.Append).jdbc()

    sparkSession.stop()
  }
}
