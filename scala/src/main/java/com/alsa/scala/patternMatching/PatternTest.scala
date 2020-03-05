package com.alsa.scala.patternMatching

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/13
  * @Time:11:12
  *
  **/
object PatternTest{
  def main(args: Array[String]): Unit = {
    val sample = new Sample
    sample.process(10000)
  }
}
class Sample{
  var max = 10000;
  def process(input:Int):Unit={
    input match {
      case max=>println(s"You matched max");
    }
  }
}
