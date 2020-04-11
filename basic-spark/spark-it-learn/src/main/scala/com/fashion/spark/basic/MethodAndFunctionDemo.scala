package com.fashion.spark.basic

object MethodAndFunctionDemo {

  //scala中方法可以传入一个函数
  def m(f:(Int,Int)=> Int):Int= {
    f(2,3)
  }

  //定义函数1
  val f1 = (x:Int,y:Int)=>x + y

  //定义函数2
  val f2 = (x:Int,y:Int)=> x * y

  def main(args: Array[String]): Unit = {

    println(m(f1))

    println(m(f2))
  }
}
