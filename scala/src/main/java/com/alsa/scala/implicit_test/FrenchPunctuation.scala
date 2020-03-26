package com.alsa.scala.implicit_test

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/3/10
  * @Time:20:20
  *
  **/
object FrenchPunctuation{
  implicit val quoteDelimiters = Delimiters("<<",">>")
}