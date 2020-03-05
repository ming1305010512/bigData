package com.alsa.spark.dataBase.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/21
  * @Time:15:31
  * @Description:mysql读取
  *
  **/
object MysqlRDD {
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("jdbcRDD")
    //创建sparkContext
    val sc = new SparkContext(sparkConf)

    //定义连接的mysql参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://node1:3306/rdd"
    val userName = "root"
    val passWd = "123456"

    //创建jdbcRDD,参数lowerBound补全sql的第一个问号，upperBound补全sql的第二个问号
    val rdd = new JdbcRDD(sc,()=>{
      Class.forName(driver)
      DriverManager.getConnection(url,userName,passWd)
    },"select * from rddtable where id>=? and id <= ?;",
      1,10,1,
      r=>(r.getInt(1),r.getString(2)))
    //打印最后的结果
    println(rdd.count())
    rdd.foreach(println)
    sc.stop();
  }
}
