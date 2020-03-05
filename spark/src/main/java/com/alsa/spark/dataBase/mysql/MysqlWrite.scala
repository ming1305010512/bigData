package com.alsa.spark.dataBase.mysql

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/21
  * @Time:15:48
  * @Description:mysql写入
  *
  **/
object MysqlWrite {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(List("Female","Male","Female"))
    data.foreachPartition(insertData)
    sc.stop()
  }

  def insertData(iterator: Iterator[String]):Unit = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://node1:3306/rdd","root","123456")
    iterator.foreach(data=>{
      val ps = conn.prepareStatement("insert into rddtable(name) values(?)")
      ps.setString(1,data)
      ps.executeUpdate()
    })
  }
}
