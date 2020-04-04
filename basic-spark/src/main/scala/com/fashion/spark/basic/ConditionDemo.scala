package com.fashion.spark.basic

object ConditionDemo {

  def main(args: Array[String]): Unit = {
    val x = 1
    val y = if( x>0 ) 1 else -1
    println(y)

    //支持混合类型表达式
    val z = if(x>1) 1 else "error"
    println(z)

    //如果没有else,相当于 if() 1 else ()
    val m = if(x > 2) 1 //m = ()
    println(m)

    //在scala中每个表达式都有返回值,Unit相当于java中的void, Unit只有一个值()

    val n = if(x>2) 1 else ()
    println(n)

    // if () else if()
    val k = if(x<0) 0
            else if(x>1) 1
            else -1
    println(k)

  }

}
