package com.fashion.spark.quick.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextApplication {

  def main(args: Array[String]): Unit = {

    //创建sparkContext
    val sparkConf:SparkConf = new SparkConf().setMaster("local").setAppName("quick-learn")

    val sc:SparkContext =  new SparkContext(sparkConf)

  }

}
