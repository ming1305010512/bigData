package com.alsa.spark.sparkCore.RddTransfomation.keyValueType

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/3/23
  * @Time:15:04
  * @Description: KeyValueType示例
  *
  **/
object keyValueType {
  def main(args: Array[String]): Unit = {
    //初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val sc = new SparkContext(sparkConf)

    //groupByKey
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
    val group = wordPairsRDD.groupByKey()
    for ((k,v)<- group.collect()){
      print("("+k+","+v+")")
    }
    var group2 = group.map(t => (t._1, t._2.sum))
    for ((k,v)<- group2.collect()){
      print("("+k+","+v+")")
    }

    wordPairsRDD.sortBy
  }
}
