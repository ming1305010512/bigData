package com.alsa.scala.implicit_test

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/3/10
  * @Time:20:27
  *
  **/
object del {
  def main(args: Array[String]): Unit = {
    println("1111")
    import FrenchPunctuation.quoteDelimiters
    quote("lalalalal")
  }
  def quote(what:String)(implicit delims:Delimiters) = delims.left+what+delims.right
}
