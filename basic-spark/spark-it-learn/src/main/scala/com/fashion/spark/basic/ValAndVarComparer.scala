package com.fashion.spark.basic

object ValAndVarComparer {

  def main(args: Array[String]): Unit = {
    // var 声明的变量是可变的
    var firstName = "hello"
    firstName ="hello"
    println(firstName)

    //val 相当于Java的final变量
    val name = "yangyuguang"
    print(name)
  }
}
