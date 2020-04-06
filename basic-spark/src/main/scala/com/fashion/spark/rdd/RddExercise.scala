package com.fashion.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RddExercise {

  def main(args: Array[String]): Unit = {

    //创建一个rdd
    val sparkConf =  new SparkConf().setAppName("rddExercise").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val sourceRdd = sc.parallelize(List(5,6,7,8,9,10))

    val rdd2 = sourceRdd.map(_*2).sortBy(x=>x,true).filter(_>5)

    println("输出rdd2")
    // rdd输出结果可以使用foreach输出
    rdd2.foreach(println)
    println("输出rdd2")


    val rdd3  = sc.parallelize(Array("a b c","e f g"))
    val rdd4= rdd3.flatMap(_.split(" "))
    rdd4.foreach(println)

    val rdd5 = sc.parallelize(List(1,2,3,4))
    val rdd6 = sc.parallelize(List(4,5,6,7))
    //union是并集
    val rdd7 = rdd5.union(rdd6)
    print("输出并集")
    rdd7.foreach(println)
    print("输出并集")
    print("输出交集")
    val rdd8 = rdd5.intersection(rdd6)
    rdd8.foreach(println)
    print("输出交集")


    //join的使用
    val rdd9 =  sc.parallelize(List(("tom",1),("jerry",3),("kitty",2)))

    val rdd10 = sc.parallelize(List(("jerry",2),("tom",1),("shuke",2)))

    //使用join 的作用： 输入: k,v 和k,w 输出 k,(v,w). 只有两个rdd同样的key才会被输出
    val rdd11 = rdd9.join(rdd10)
    println("输出join")
    rdd11.foreach(println)
    println("输出join")

    //使用groupBy, 返回结果 是k,(v1,v2)
    println("输出groupByKey")
    val rdd12 = rdd9 union rdd10
    rdd12.groupByKey().foreach(println)
    println("输出groupByKey")

    println("输出cogroup")

    val rdd13 = sc.parallelize(List(("tom",1),("tom",2),("jerry",3),("kitty",2)))
    val rdd14 = sc.parallelize(List(("jerry",2),("tom",1),("jim",2)))

    /**
      * 返回结果：第一步将自身的rdd的v聚合，然后作为一个computeBuffer, 与另外一个rdd中的结果合并
      * (jim,(CompactBuffer(),CompactBuffer(2)))
      * (tom,(CompactBuffer(1, 2),CompactBuffer(1)))
      * (jerry,(CompactBuffer(3),CompactBuffer(2)))
      * (kitty,(CompactBuffer(2),CompactBuffer()))
      */
    val rdd15 = rdd13.cogroup(rdd14)
    rdd15.foreach(println)
    println("输出cogroup")

    println("输出reduce")
    val rdd16 = sc.parallelize(List(1,2,3,4,5))
    val rdd17 = rdd16.reduce(_+_) //reduce简单来说，就是返回一个整数的和
    println(rdd17)
    println("输出reduce")

    println("输出reduceByKey")//输入一个(k,v)的rdd，返回一个(k,v)的rdd ,task个数可以通过参数指定
    val rdd18 =sc.parallelize(List(("tom",1),("tom",4),("jerry",3)))
    val rdd19 = rdd18.reduceByKey(_+_)//第一个函数作用在(k,v)中的v
    rdd19.foreach(println)
    println("输出reduceByKey")



    val rdd20 = rdd19.map(t=>(t._2,t._1))
    println("将函数倒置")
    rdd20.foreach(println)
    val rdd21 = rdd20.sortByKey(false)
    val rdd22 = rdd21.map(t=>(t._2,t._1))
    rdd22.foreach(println)
    println("将函数倒置")
    println("输出sortByKey")
    println("输出sortByKey")

    println("增加分区")
    val rdd23 = sc.parallelize(1 to 10,3)
    rdd23.foreach(println)
    val size0 = rdd23.partitions.size
    println("size0: " + size0)
    val size1 = rdd23.repartition(2).partitions.size
    println("size1: " +size1)
    println("增加分区")
    sc.stop()
  }
}
