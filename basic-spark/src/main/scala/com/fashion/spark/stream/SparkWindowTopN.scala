package com.fashion.spark.stream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkWindowTopN {

  def main(args: Array[String]): Unit = {
    //1.创建sparkStreaming
    val sparkConf = new SparkConf().setAppName("sparkStreamingExercise")
      .setMaster("local[2]")

    //2. 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")
    //设置checkpoint的路径
    sc.setCheckpointDir("./ck")

    //获取tcp中的数据
    val streamingContext = new StreamingContext(sc,Seconds(5))

    //sparkStream监听一个端口
    val lines = streamingContext.socketTextStream("192.168.5.128",9999)

    val words = lines.flatMap(_.split(" "))

    val workAndOne = words.map((_,1))

    //效果： a        ===>      b    ===>    c   ========>   空         ===> 空
    //    第一个五秒       第二个五秒       第三个五秒       第四个五秒        第五个五秒
    //计算结果： (a,1)      (a,1)(b,1 )      (b,1)(c,1 )       (c,1)            空
    val result = workAndOne.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(5))

    //获取一段时间内的前3个热词
    val data = result.transform(rdd=>{
      val dataRdd = rdd.sortBy(t=>t._2,false)
      val sortResult = dataRdd.take(3)

      println("----------print top 3 begin")
      sortResult.foreach(println)
      println("----------print top 3 end")

      dataRdd
    })

    data.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
