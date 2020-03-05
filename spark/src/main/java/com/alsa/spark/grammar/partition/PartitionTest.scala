package com.alsa.spark.grammar.partition

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/21
  * @Time:14:39
  * @Description: 获取RDD分区
  *
  **/
object PartitionTest {
  def main(args: Array[String]): Unit = {
    //初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val sc = new SparkContext(sparkConf)

    //创建一个pairRDD
    val pairs = sc.parallelize(List((1,1),(2,2),(3,3)))
    //查看rdd的分区器
    println(pairs.partitioner)//为None
    //导入HashPartitioner
    import org.apache.spark.HashPartitioner
    //使用HashPartitioner进行重新分区
    val partitioned = pairs.partitionBy(new HashPartitioner(2))
    //查看重新分区后RDD的分区器
    println(partitioned.partitioner)

    //关闭与spark的连接
    sc.stop()
  }
}
