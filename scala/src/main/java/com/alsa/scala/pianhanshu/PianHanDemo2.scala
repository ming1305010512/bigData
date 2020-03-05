package com.alsa.scala.pianhanshu

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/13
  * @Time:13:43
  *
  **/
object PianHanDemo2{
  def main(args: Array[String]): Unit = {
    var list = List(1,2,3,4,"abc");
    println(list.map(addOne))
    (1 to 9).map("*" * _).foreach(println _)
    println("*".*(2))
  }

  def addOne(i:Any):Any = {
    i match {
      case x:Int => x+1
      case _ =>
    }
  }
}
