package com.fashion.spark.basic

/**
  * scala中的+ - * / % 这些操作符简单来说，都可以看做是方法
  */
object OperatorToMethod {
  def main(args: Array[String]): Unit = {

    val a = 10
    val b = 20
    println(a + b)
    println(a.+(b))

    println(a * b)
    println(a.*(b))

    println(a / b)
    println(a./(b))

    println(a % b)
    println(a.%(b))
  }
}
