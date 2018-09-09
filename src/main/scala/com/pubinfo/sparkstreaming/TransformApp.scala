package com.pubinfo.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /**
      * 构建黑名单
      */
    val blacks = List("zs", "ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val lines = ssc.socketTextStream("localhost", 6789)
    //map是rdd里面的一个个，transform是一个个rdd
    val clicklog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        //getOrElse存在就返回，否则返回括号里的
        .filter(x=> x._2._2.getOrElse(false) != true)
        .map(x=>x._2._1)
    })

    clicklog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
