package com.alsa.spark.sparkCore.RddTransfomation.valueType

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/3/23
  * @Time:14:27
  * @Description: value 类型示例
  *
  **/
object valueType {
  def main(args: Array[String]): Unit = {
    //初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(1 to 10,2)
    rdd.pip
  }
}
