package com.lixiang.educationOnlie.qz.controller

import com.lixiang.educationOnlie.qz.dao.AdsQzDao
import com.lixiang.educationOnlie.qz.service.AdsQzService
import com.lixiang.educationOnlie.utils.HiveUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AdsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "basichadoop")
    val conf: SparkConf = new SparkConf().setAppName("DwdController").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext

    HiveUtils.openDynamicPartition(spark) //开启动态分区
    HiveUtils.OpenComperssion(spark) //开启压缩
    HiveUtils.useSnappyCompression(spark) //使用snappy压缩
    val dt:String = "20190722"
    AdsQzService.getTarget(spark,dt)
    AdsQzService.getTargetApi(spark,dt)
  }
}
