package com.lixiang.educationOnlie.member.controller

import com.lixiang.educationOnlie.member.service.EtlDataService
import com.lixiang.educationOnlie.utils.HiveUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DWDMemberController {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:/hadoop-common-2.2.0-bin-master")
    System.setProperty("HADOOP_USER_NAME","basichadoop")
    val conf: SparkConf = new SparkConf()
    conf.setAppName("dwd_member_import").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()//开启spark对hive的支持（可以兼容hive的语法了）
      .getOrCreate()
    val ssc: SparkContext = spark.sparkContext

    HiveUtils.openDynamicPartition(spark)
    HiveUtils.SetMaxPartion(spark)
    HiveUtils.useSnappyCompression(spark)

    EtlDataService.etlBaseAdLog(ssc, spark) //导入基础广告表数据
    EtlDataService.etlBaseWebSiteLog(ssc, spark) //导入基础网站表数据
    EtlDataService.etlMemberLog(ssc, spark) //清洗用户数据
    EtlDataService.etlMemberRegtypeLog(ssc, spark) //清洗用户注册数据
    EtlDataService.etlMemPayMoneyLog(ssc, spark) //导入用户支付情况记录
    EtlDataService.etlMemVipLevelLog(ssc, spark) //导入vip基础数据

  }
}
