package com.lixiang.educationOnlie.qz

import com.lixiang.educationOnlie.qz.service.DwsQzService
import com.lixiang.educationOnlie.utils.HiveUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "basichadoop")
    val conf: SparkConf = new SparkConf().setAppName("DwsController").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext

    HiveUtils.openDynamicPartition(spark) //开启动态分区
    HiveUtils.OpenComperssion(spark) //开启压缩
    HiveUtils.useSnappyCompression(spark) //使用snappy压缩

    val dt:String = "20190722"
    DwsQzService.saveDwsQzChapter(spark,dt)
    DwsQzService.saveDwsQzCourse(spark,dt)
    DwsQzService.saveDwsQzMajor(spark,dt)
    DwsQzService.saveDwsQzPaper(spark,dt)
    DwsQzService.saveDwsQzQuestionTpe(spark,dt)
    DwsQzService.saveDwsUserPaperDetail(spark,dt)
  }
}
