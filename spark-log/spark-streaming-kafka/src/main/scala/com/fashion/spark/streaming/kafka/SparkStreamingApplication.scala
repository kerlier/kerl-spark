package com.fashion.spark.streaming.kafka

import com.fashion.spark.streaming.dao.HbaseDao
import com.fashion.spark.streaming.domain.{ClickCount, ClickLog}
import com.fashion.spark.streaming.util.{DateUtil, HtmlUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreamingApplication {

  def main(args: Array[String]): Unit = {

    if(args.length != 4)
    {
      println("")
      System.exit(1)
    }
    val Array(zkHost,groupId, topics,numThread) = args

    val topicMap = topics.split(",").map((_, numThread.toInt)).toMap

//    println(zkHost)
//    println(groupId)
//    println(topics)
//    println(numThread)
//    println(topicMap)

    val sparkConf = new SparkConf().setAppName("SparkStreamingWithKafka").setMaster("local[2]")

    //创建sparkStreaming
    val scc = new StreamingContext(sparkConf, Seconds(60))

    val messages = KafkaUtils.createStream(scc, zkHost, groupId ,topicMap)

//    messages.map(_._2).count().print

    val logs = messages.map(_._2)

    val cleanData = logs.map(line => {
      //55.63.10.72	2020-06-07 00:52:02	"GET /course/list HTTP/1.1"	-	401
      val infos = line.split("\t")

      val url = infos(2).split(" ")(1)

      val courseId = HtmlUtil.getCourseId(url)

      ClickLog(infos(1),DateUtil.parseToMinute(infos(1)), courseId,infos(4).toInt, infos(3))
    }).filter(clickLog => clickLog.courseId!=0)


    //统计到今天为止课程的访问量
    cleanData.map(x=>{

      val day = x.time.substring(0,8)

      val row_key = day+"_" + x.courseId

      (row_key, 1)
    }).reduceByKey(_+_).foreachRDD(
      rdd=>{
        rdd.foreachPartition(partitionRecords => {
          val list = new ListBuffer[ClickCount]
          partitionRecords.foreach(pair => {
            list.append(ClickCount(pair._1, pair._2))
          })
          HbaseDao.save(list)
        })
      }
    )


    cleanData.print()
    scc.start()
    scc.awaitTermination()
  }

}
