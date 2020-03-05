package com.alsa.scala.other

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/13
  * @Time:16:25
  *
  **/
object Test{
  def compute(input:Int):Either[String,Double] = {
    if (input>0)
      Right(math.sqrt(input))
    else
      Left("Error computing,invalid input")
  }

  def displayResult(result:Either[String,Double]):Unit = {
    result match {
      case Left(result) => println(s"error=$result")
      case Right(value) => println(s"result=$value")
    }
  }

  def main(args: Array[String]): Unit = {
    displayResult(compute(10))
    displayResult(compute(-10))
  }
}
