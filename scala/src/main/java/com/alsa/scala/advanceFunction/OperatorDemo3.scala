package com.alsa.scala.advanceFunction

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/13
  * @Time:15:12
  *
  **/
object OperatorDemo3{
  def main(args: Array[String]): Unit = {
    val f:Int=>Int = foo(10,_:Int);
    println(f(20))
    println(f(30))
  }
  def foo(a: Int, b: Int):Int = {
    a*b
  }
}
