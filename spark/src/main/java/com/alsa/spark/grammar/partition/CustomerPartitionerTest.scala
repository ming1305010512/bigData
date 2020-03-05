package com.alsa.spark.grammar.partition

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/21
  * @Time:15:02
  *
  **/
object CustomerPartitionerTest {
  def main(args: Array[String]): Unit = {
    //初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(Array((1,1),(2,2),(3,3),(4,4),(5,5),(6,6)))
    val par = data.partitionBy(new CustomerPartitioner(2))
    //查看重新分区后的数据分布
    val result = par.mapPartitionsWithIndex((index,items)=>items.map((index,_))).collect()
    result.foreach(println(_))

    sc.stop();
  }
}
