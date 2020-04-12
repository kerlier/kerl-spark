package com.fashion.spark.quick.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PairRddExercise {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setMaster("local").setAppName("quick-pair-rdd-learn")
    val sc:SparkContext =  new SparkContext(sparkConf)

//    val pairRdd:RDD[(String,String)] = createStrPairRdd(sc)
//    pairRdd.foreach(println)

//    useReduceByKey(sc)

//    useGroupByKey(sc)

//    useMapValues(sc)

//    useFlatMapValues(sc)

//    useSubtractByKey(sc)

//    useJoin(sc)

//    useKeyAndValue(sc)

//    filterValue(sc)

//    useMapValueAndReduceByKey(sc)

    useCountByValue(sc)
    sc.stop()
  }

  def createStrPairRdd(sc:SparkContext):RDD[(String,String)]={
    val input:RDD[(String)] = sc.parallelize(List("hello world","nice good"))
    val mapRdd :RDD[(String,String)] = input.map(x=>(x.split(" ")(0),x))
    mapRdd
  }

  def useReduceByKey(sc:SparkContext):Unit={
    val lineRdd:RDD[(String)] = sc.parallelize(List("hello world","ni hao"))
    val wordRdd:RDD[(String)] = lineRdd.flatMap(_.split(" "))
    val wordCountRdd:RDD[(String,Int)] = wordRdd.map(x=>(x,1))
    val allWordRdd:RDD[(String,Int)] = wordCountRdd.reduceByKey((x,y)=>(x+y))
    val firstWordCount:(String,Int) = allWordRdd.first()
    println(firstWordCount)
  }

  /**
    * groupBykey 根据pairRdd中的key进行分组，然后将value整合到一个迭代器中
    * @param sc
    */
  def useGroupByKey(sc:SparkContext):Unit={
    val lineRdd:RDD[(String)] = sc.parallelize(List("hello world","ni hao","hello ni hao"))
    val wordRdd:RDD[(String)] = lineRdd.flatMap(_.split(" "))
    val wordCountRdd:RDD[(String,Int)] = wordRdd.map(x=>(x,1))
    val wordGroup:RDD[(String,Iterable[Int])] = wordCountRdd.groupByKey()
    val top10: Array[(String,Iterable[Int])] = wordGroup.take(10)
    top10.foreach(println)
  }

  /**
    * mapValues 对pairRdd中的value进行操作，key值不变
    * @param sc
    */
  def useMapValues(sc:SparkContext):Unit={
    val lineRdd:RDD[(String)] = sc.parallelize(List("hello world","ni hao","hello ni hao"))
    val wordRdd:RDD[(String)] = lineRdd.flatMap(_.split(" "))
    val wordCountRdd:RDD[(String,Int)] = wordRdd.map(x=>(x,1))

    println("输出原来的rdd")
    wordCountRdd.foreach(println)

    val finalRdd:RDD[(String,Int)] = wordCountRdd.mapValues(x=>x+1)
    finalRdd.foreach(println)
  }

  /**
    * flatMapValues 对pairRdd中的value进行操作，value增多后，key不变，会追加到原来的rdd中
    * @param sc
    */
  def useFlatMapValues(sc:SparkContext):Unit={
    val lineRdd:RDD[(String)] = sc.parallelize(List("hello world","ni hao","hello ni hao"))
    val wordRdd:RDD[(String)] = lineRdd.flatMap(_.split(" "))
    val wordCountRdd:RDD[(String,Int)] = wordRdd.map(x=>(x,1))

    println("输出原来的rdd")
    wordCountRdd.foreach(println)

    val finalRdd:RDD[(String,Int)] = wordCountRdd.flatMapValues(x=>(x to 5))
    finalRdd.foreach(println)
  }

  /**
    * 保留 rdd1中，但不在rdd2中的元素
    * @param sc
    */
  def useSubtractByKey(sc:SparkContext):Unit={
    val lineRdd:RDD[(String)] = sc.parallelize(List("hello world","ni hao","hello ni hao"))
    val wordRdd:RDD[(String)] = lineRdd.flatMap(_.split(" "))
    val wordCountRdd:RDD[(String,Int)] = wordRdd.map(x=>(x,1))

    val lineRdd1:RDD[(String)] = sc.parallelize(List("hello world"))
    val wordRdd1:RDD[(String)] = lineRdd1.flatMap(_.split(" "))
    val wordCountRdd1:RDD[(String,Int)] = wordRdd1.map(x=>(x,1))

    val subtractRdd:RDD[(String,Int)] =wordCountRdd.subtractByKey(wordCountRdd1)

    val top10:Array[(String,Int)] = subtractRdd.take(10)

    top10.foreach(println)
  }

  /**
    * 输出结果:
    * (3,(4,9))
    * (3,(6,9))
    * @param sc
    */
  def useJoin(sc:SparkContext):Unit={
    val lineRdd1:RDD[(Int,Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))
    val lineRdd2:RDD[(Int,Int)] = sc.parallelize(List((3,9)))
    val finalRdd:RDD[(Int,(Int,Int))] = lineRdd1.join(lineRdd2)
    finalRdd.foreach(println)
  }

  def useKeyAndValue(sc:SparkContext):Unit={
    val lineRdd1:RDD[(Int,Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))
    //在scala中keys 以及values都不要使用括号
    val keys:RDD[Int] = lineRdd1.keys
    keys.foreach(println)

    val values = lineRdd1.values
    values.foreach(println)
  }

  def filterValue(sc:SparkContext):Unit={
    val mapRdd:RDD[(String,String)] = createStrPairRdd(sc)
    //pairRdd filter使用的是filter参数使用的是大括号,关键词用的是case
    val filterRdd:RDD[(String,String)] = mapRdd.filter{case (key,value)=> value.length<20}

    filterRdd.foreach(println)
  }

  /**
    * 求每个key对应的平均值
    */
  def useMapValueAndReduceByKey(sc:SparkContext):Unit={
    val lineRdd1:RDD[(String,Int)] = sc.parallelize(List(("panda",2),("panda",4),("pink",6),("pink",4)))
    val mapRdd:RDD[(String,(Int,Int))] = lineRdd1.mapValues(x=> (x,1))
    val sumRdd:RDD[(String,(Int,Int))] = mapRdd.reduceByKey((x,y)=> (x._1+ y._1,x._2+y._2))

    sumRdd.foreach(println)

    //对值进行操作
    val avgRdd:RDD[(String,Double)] = sumRdd.mapValues(y=>y._1/y._2)

    avgRdd.foreach(println)
  }

  /**
    * 使用countByValue可以实现单词计数
    * @param sc
    */
  def useCountByValue(sc:SparkContext):Unit={
    val lineRdd:RDD[(String)] = sc.parallelize(List("hello world","ni hao","hello ni hao"))
    val wordRdd:RDD[(String)] = lineRdd.flatMap(_.split(" "))

    //导入map的依赖包
    import  scala.collection._
    val countRdd:Map[String,Long]= wordRdd.countByValue()
    print(countRdd)
  }
}
