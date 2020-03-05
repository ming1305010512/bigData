package com.alsa.spark.sparkCore.advertisingClickCount

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Created with IDEA
  * @author:longming
  * @Date:2020/1/19
  * @Time:14:28
  * @Description: 统计出每一个省份广告被点击次数的top3
  *
  **/
object Practice {
  def main(args: Array[String]): Unit = {
    //初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val sc = new SparkContext(sparkConf)

    //读取数据生成RDD：TS  ,Province,City,User,AD
    val line = sc.textFile("C:\\学习\\项目\\学习项目\\bigData\\spark\\src\\main\\java\\com\\alsa\\spark\\sparkCore\\advertisingClickCount\\agent.log")
    //按照最小粒度聚合：((province,AD),1)
    val provinceAdAndOne = line.map{x=>
      val fields:Array[String] = x.split(" ");
      ((fields(1),fields(3)),1)
    }
    //计算每个省中每个广告被点击的总数((province,AD),sum)
    val provinceAdToSum = provinceAdAndOne.reduceByKey(_+_)
    //将省份作为key，广告加点击数为value：(province,(AD,sum))
    val provinceToAddSum = provinceAdToSum.map(x=>(x._1._1,(x._1._2,x._2)))
    //将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)))
    val provinceGroup = provinceToAddSum.groupByKey()
    //对同一个省份所有广告的集合进行排序并取前三条，排序规则为广告点击次数
    val provinceAdTop3 = provinceGroup.mapValues{x=>x.toList.sortWith((x,y)=>x._2>y._2).take(3)}
    //将数据拉取到Driver端并打印
    provinceAdTop3.collect().foreach(println)
    //关闭与spark的连接
    sc.stop()
  }
}
