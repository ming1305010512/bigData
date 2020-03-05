package com.alsa.spark.demo1

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/16
  * @Time:15:53
  * @Description: 字符串个数
  *
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    //设置sparkConf并设置app名称
    val conf = new SparkConf().setAppName("WC");
    //创建SparkContext,该对象是提交Spark App的入口
    val sc = new SparkContext(conf);
    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile(args(0)).flatMap(_.split("  ")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false).saveAsTextFile(args(1))
    //关闭连接
    sc.stop()
  }
}
