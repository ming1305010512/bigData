package com.alsa.spark.grammar.map

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/19
  * @Time:14:06
  *
  **/
object MapTest {
  def main(args: Array[String]): Unit = {
    val a = Array(1,2)
    val b = a.map{case i:Int=>i+1}
    b.foreach(println(_))
    a.map(_%2)
  }
}
