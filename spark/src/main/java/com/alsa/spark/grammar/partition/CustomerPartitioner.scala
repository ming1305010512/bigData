package com.alsa.spark.grammar.partition

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/21
  * @Time:14:58
  * @Description:自定义分区
  *
  **/
class CustomerPartitioner(numParts:Int) extends org.apache.spark.Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val ckey:String = key.toString;
    ckey.substring(ckey.length-1).toInt%numParts
  }
}
