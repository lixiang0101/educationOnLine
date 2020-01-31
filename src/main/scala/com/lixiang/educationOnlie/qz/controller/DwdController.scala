package com.lixiang.educationOnlie.qz.controller

import com.lixiang.educationOnlie.qz.service.EtlDataService
import com.lixiang.educationOnlie.utils.HiveUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "basichadoop")
    val conf: SparkConf = new SparkConf().setAppName("DwdController").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext

    HiveUtils.openDynamicPartition(spark) //开启动态分区
    HiveUtils.OpenComperssion(spark) //开启压缩
    HiveUtils.useSnappyCompression(spark) //使用snappy压缩

    EtlDataService.etlDataChapter(spark,ssc)
    EtlDataService.etlQzBusiness(ssc,spark)
    EtlDataService.etlQzCenter(ssc,spark)
    EtlDataService.etlQzCenterPaper(ssc,spark)
    EtlDataService.etlQzChapterList(ssc,spark)
    EtlDataService.etlQzCourse(ssc,spark)
    EtlDataService.etlQzCourseEdusubject(ssc,spark)
    EtlDataService.etlQzMajor(ssc,spark)
    EtlDataService.etlQzMemberPaperQuestion(ssc,spark)
    EtlDataService.etlQzPaper(ssc,spark)
    EtlDataService.etlQzPaperView(ssc,spark)
    EtlDataService.etlQzPoint(ssc,spark)
    EtlDataService.etlQzPointQuestion(ssc,spark)
    EtlDataService.etlQzQuestion(ssc,spark)
    EtlDataService.etlQzQuestionType(ssc,spark)
    EtlDataService.etlQzSiteCourse(ssc,spark)
    EtlDataService.etlQzWebsite(ssc,spark)
  }
}
