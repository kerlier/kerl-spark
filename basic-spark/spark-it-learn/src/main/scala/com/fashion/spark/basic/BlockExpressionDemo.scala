package com.fashion.spark.basic

/**
  * 块表达式，最后一个表达式的值就是整个块表达式的值
  */
object BlockExpressionDemo {

  def main(args: Array[String]): Unit = {
    val a = 10
    val b = 20
    val result = {
         a + b
    }

    print(result)
  }

}
