package com.fashion.spark.sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
object SqlExercise {
  //正式开发环境中使用这种方式来开发，不要使用case class
  val schema = StructType(
    Seq(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)
    )
  )

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("sqlExercise")
    sparkConf.setMaster("local")

    //sparkSql操作的是dataFrame ,简单来说 ，rdd是一个person ,dataFrame是 (int,string,int)的集合

    //创建sparkSession
    val sparkSession1 = SparkSession.builder()
      .appName("SparkSQLDemo")
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    val rowRdd = sparkSession1.sparkContext
                             .textFile("hdfs://node1:9000/spark/person.txt")
                             .map(_.split(" "))
                             .map(x=>(Row(x(0).toInt,x(1),x(2).toInt)))// 这里使用Row

    val personDataFrame:DataFrame = sparkSession1.createDataFrame(rowRdd,schema)

    //展示表中的数据
    personDataFrame.show()

    //执行sparkSql语句
    //1 查询name字段的值
    personDataFrame.select("name").show()

    //2 查询name, age的值
    personDataFrame.select("name","age" ).show()

    personDataFrame.select("id","name","age").show(2)

    //打印表结构
    personDataFrame.printSchema()

    //filter操作，根据某个字段进行过滤, 输入一个表达式就可以了
    personDataFrame.filter("age > 35").show()

    //按照年龄分组，然后计算年龄的个数
    personDataFrame.groupBy("age").count().show()


    //将dataFrame注册成一张表，然后可以sql语句
    personDataFrame.createOrReplaceTempView("person")

    val nameDataFrame= sparkSession1.sql("select name from person")
    nameDataFrame.show()

    sparkSession1.sql("desc person").show()

    sparkSession1.sql("select name from  person where age >35").show()

    sparkSession1.stop()

  }

  //case class可以看做是mysql表结构的schema
  case class Person(id: Int, name:String, age:Int  )

}
