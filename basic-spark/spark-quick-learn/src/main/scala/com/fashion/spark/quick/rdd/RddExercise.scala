package com.fashion.spark.quick.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

/**
  * rdd有两种操作:
  * 第一个是转换操作(transform)
  * 第二个是行动操作(action)
  */
object RddExercise {

  def main(args: Array[String]): Unit = {
    //创建sparkContext
    val sparkConf:SparkConf = new SparkConf().setMaster("local").setAppName("quick-learn")

    val sc:SparkContext =  new SparkContext(sparkConf)

    sc.setLogLevel("INFO")

//    getSquared(sc)

//    println("first: " + useFlatMap(sc))

//    useDistinct(sc)

//    useIntersection(sc)

//    useSubtract(sc)

//    useSample(sc)

//    useReduce(sc)

//    useFold(sc)

    useAggregate(sc)
    sc.stop()
  }

  /**
    * 可以返回与Rdd元素不同类型的值
    * @param sc
    */
  def useAggregate(sc:SparkContext):Unit={
    val input1 = sc.makeRDD(List(1,2,3),2)

    val result = input1.aggregate((0,0))((x,y)=>(x._1+y,x._2+1),(x,y)=>(x._1+y._1,x._2+y._2))

    println(result)

    val avg = result._1/result._2.toDouble

    println(avg)
  }

  def useFold(sc:SparkContext):Unit={
    val input1 = sc.makeRDD(List(1,2,3),2)
    val partitions:Array[Partition] = input1.partitions
    println("分区数:" + partitions.size)



    //zeroValue是0的时候，返回结果是36
    //zeroValue是10的时候，返回结果是56,多了20
    //算子其实就是先对rdd分区的每一个分区进行使用op函数，在调用op函数过程中将zeroValue参与计算，最后在对每一个分区的结果调用op函数，同理此处zeroValue再次参与计算！
    //fold执行操作分为两步：
    //1. 对每个分区进行op操作，zeroValue参加运算
    //2. 所有分区的结果汇总后,进行运算,zeroValue也参加运算
    val foldNum:Int = input1.fold(10)(_*_)

    //乘法
    //第一步:每个分区进行运算
    //1*10 =10
    //10*2 =20
    //20*3=60

    //第二步: 所有分区的结果进行运算
    //60*10 =600

    //两个分区的结果是6000
    //1*10 =10 10*2 =20
    //3*10=30
    //30*20=600

    //600*10 =6000

    /**
      * 1、[1,2,3,4], zeroValue = 10
      *
      * 2、currentVal = 1， zeroValue = 1
      *
      * 3、currentVal = 2， zeroValue = 2
      *
      * 4、currentVal = 3， zeroValue = 4
      *
      * 5、currentVal = 4， zeroValue = 7
      * num= 16+4
      *
      */
    println("foldNum:" + foldNum)
  }

  def useReduce(sc:SparkContext):Unit={
    val input1 = sc.parallelize(List(1,2,3,4))

    //(x,y) 表达式：x表示返回，y表示每个遍历的元素
    val num:Int = input1.reduce((x,y)=>x+y)
//    val num:Int = input1.reduce(_+_)

    println("sum:"+ num)
  }

  def useSample(sc:SparkContext):Unit={
    val input1 = sc.parallelize(List(1,2,3,4))

    val sampleRdd:RDD[(Int)] = input1.sample(false,0.5)
    sampleRdd.foreach(println)
  }

  def useSubtract(sc:SparkContext):Unit={
    val input1 = sc.parallelize(List(1,2,3,4))
    val input2 = sc.parallelize(List(3,4,5,6))

    //返回只存在input1中，但不在input2中的元素
    val subtractRdd: RDD[(Int)]= input1.subtract(input2)

    subtractRdd.foreach(println)//返回应该是1,2
  }

  //intersection只返回两个rdd中都有的元素
  def useIntersection(sc:SparkContext):Unit={

    val input1 = sc.parallelize(List(1,2,3,4))
    val input2 = sc.parallelize(List(3,4,5,6))

    val interSectionRdd:RDD[(Int)] = input1.intersection(input2)

    interSectionRdd.foreach(println)
  }

  //使用distinct进行去重，distinct会引发数据混洗
  def useDistinct(sc:SparkContext):Unit={

    val numberInput = sc.parallelize(List(1,1,1,1))
    val strUnit = sc.parallelize(List("i","i","i","i"))

    val result1:Array[(Int)] = numberInput.distinct().collect()
    val result2:Array[(String)] = strUnit.distinct().collect()

    result1.foreach(println)
    result2.foreach(println)
  }

  //使用flatMap，传入一个参数，返回多个参数
  def useFlatMap(sc:SparkContext):String={

    val input = sc.parallelize(List("i love lvshuzhen","hello world"))
    val words = input.flatMap(_.split(" "))
    words.first()
  }

  //求数组各值的平方
  def getSquared(sc:SparkContext):Unit={
    val input = sc.parallelize(List(1,2,3,4))
    val squared = input.map(x=>x*x).collect()
    println(squared.mkString(","))
  }

  def basicAction(sc:SparkContext):Unit={
    val inputRdd:RDD[(String)]  = sc.textFile("E:\\git\\spark\\basic-spark\\spark-quick-learn\\data\\log.txt")

    //获取第一行
    val firstLine = inputRdd.first()

    println(firstLine)

    //transform: filter操作
    //    val filterRdd:RDD[(String)]= inputRdd.filter(line=>line.startsWith("h"))

    val errorAndWarnlog:RDD[(String)] = inputRdd.filter(line=>line.contains("error")||line.contains("warn"))

    //    filterRdd.persist()

    errorAndWarnlog.foreach(println)

    val count = errorAndWarnlog.count()
    println("满足条件的行数是" + count)

    //take获取少量的参数
    //collect是收集所有的参数，这个命令尽量小心使用。
    errorAndWarnlog.take(2).foreach(println)
  }


}
