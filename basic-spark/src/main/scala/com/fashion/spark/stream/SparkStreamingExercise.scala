package com.fashion.spark.stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 读取socket中的数据，然后进行单词计数
  */
object SparkStreamingExercise {

  def main(args: Array[String]): Unit = {
    //1.创建sparkStreaming
    val sparkConf = new SparkConf().setAppName("sparkStreamingExercise")
      .setMaster("local[2]")

    //2. 创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    //3. 构建sparkStreamContext对象.
    //严格意思来说，sparkStreaming不算实时的流处理，它是由有一个时间的间隔
    // 这里的Seconds(5)每隔五秒计算一次，然后将每个批次的结果输出
    val streamingContext = new StreamingContext(sc,Seconds(5))
    
    //sparkStream监听一个端口
    val lines = streamingContext.socketTextStream("192.168.5.128",9999)

    val words = lines.flatMap(_.split(" "))

    val workAndOne = words.map((_,1))

    val result = workAndOne.reduceByKey(_+_)

    //输出结果,这里的结果是输出的每一个批次的结果，而不是将所有的批次结果进行累加
    result.print()

    //启动stream
    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
