package com.fashion.spark.stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现将所有的批次结果进行累加
  */
object SparkStreamingTcpTotal {
  /**
    * newValues表示当前的批次相同的单词的所有的1
    * runingCount表示历史数据库中key的总和
    * @param newValues
    * @param runningCount
    * @return
    */
  def updateFunction(newValues:Seq[Int], runningCount:Option[Int]):Option[Int] ={
    val newCount = runningCount.getOrElse(0) +newValues.sum

    Some(newCount)
  }


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

    val wordAndOne = lines.flatMap(_.split(" ")).map((_,1))

    //使用updateStateByKey来更新状态

    val result = wordAndOne.updateStateByKey(updateFunction)

    result.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
