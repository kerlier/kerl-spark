package com.fashion.spark.kafkastream

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWithKafka {

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

    //设置kafkaParams
    val kafkaParams = Map("metadata.broker.list"->"192.168.5.128:9092","group.id"->"kafka_Direct")

    //定义topic
    val topics = Set("kafka_spark")

    //通过kafkaUtil.creat..来接收数据
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](streamingContext,kafkaParams,topics)

    //获取kafka中的topic数据
    val topicData:DStream[String] = kafkaStream.map(_._2)

    val wordAndOne = topicData.flatMap(_.split(" ")).map((_,1))

    val result:DStream[(String,Int)] = wordAndOne.reduceByKey(_+_)

    result.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
