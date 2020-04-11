package com.fashion.spark.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
// 查看结果 hdfs dfs -cat /spark_out/part*
object WorkCountApplication {

  def main(args: Array[String]): Unit = {

    var input = args(0)

    val output = args(1)

    val sparkConf: SparkConf = new SparkConf().setAppName("wordCount")

    val sc:SparkContext = new SparkContext(sparkConf)

    val file:RDD[String] = sc.textFile(input) 

    //对每一行文件尽心切分
    val words:RDD[String] = file.flatMap(_.split(" "))

    val wordAndOne:RDD[(String,Int)] = words.map(x=>(x,1))

    val result:RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)

    result.saveAsTextFile(output)

    sc.stop()

  }
}
