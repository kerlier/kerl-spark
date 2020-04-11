package com.fashion.spark.basic

object ForDemo {
  def main(args: Array[String]): Unit = {

    //1 to 10 返回结果:Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println( 1 to 10)
    for (i <- 1 to 10 ){
      println(i)
    }

    //创建array数组
    val array = Array("a","b","c")
    for(i<-array){
      println(i)
    }

    //每个生成器都可以带一个条件。双层for循环带添加
    for(i <- 1 to 3 ; j <- 1 to 3  if i!=j){
      println("i:" + i+", j:"+j )
      //i:1, j:2
      //i:1, j:3
      //i:2, j:1
      //i:2, j:3
      //i:3, j:1
      //i:3, j:2
    }

    //单层for循环带上条件
    for (i <- 1 to 10 if i%2 ==0 ){
      println(i)
    }

    //yield. 如果循环体以yield开始，这个循环返回一个集合
    val  v = for(i<-1 to 10) yield  i*10
    println(v)

  }

}
