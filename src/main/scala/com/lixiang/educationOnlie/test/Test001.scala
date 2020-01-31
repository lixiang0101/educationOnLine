package com.lixiang.educationOnlie.test

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lixiang.educationOnlie.utils.HiveUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Test001 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/hadoop-common-2.2.0-bin-master")
    val conf: SparkConf = new SparkConf()
    val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val ssc: SparkContext = spark.sparkContext

    //HiveUtils.openDynamicPartition(spark)
    //HiveUtils.SetMaxPartion(spark)
    //HiveUtils.useSnappyCompression(spark)

    spark.sql("select distinct dt from dwd.dwd_member_regtype limit 1000").show()

  }
}
