package com.fashion.spark.basic

/**
  * 定义方法
  */


object MethodDemo {
  //定义方法,每个参数都需要指定类型，返回值的类型也需要指定-
  def sum(x:Int,y:Int):Int= {
    x + y
  }

  //函数,相当于java中Lama表达式
  val f1=(x:Int,y:Int) => x+y

  def main(args: Array[String]): Unit = {
    println(sum(1,2))

    println(f1(1,2))
  }




}

