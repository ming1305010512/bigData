package com.alsa.spark.dataBase.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/21
  * @Time:16:19
  * @Description:从HBase中读取数据
  *
  **/
object HBaseSpark {

  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)
    val conf:Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","node1,node2,node3")
    conf.set(TableInputFormat.INPUT_TABLE,"rddtable")

    //从HBASE读取数据形成RDD
    val hbaseRDD:RDD[(ImmutableBytesWritable,Result)] = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val count:Long = hbaseRDD.count()
    println(count)
    //对hbaseRDD进行处理
    hbaseRDD.foreach{
      case (_,result) =>
        val key:String = Bytes.toString(result.getRow)
        val name:String = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))
        val color:String = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("color")))
        println("RowKey:"+key+",Name:"+name+",Color:"+color)
    }
    //关闭连接
    sc.stop()
  }
}
