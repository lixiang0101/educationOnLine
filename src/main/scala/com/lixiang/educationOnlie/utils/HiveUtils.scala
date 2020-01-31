package com.lixiang.educationOnlie.utils

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object HiveUtils {
  /**
   *设置动态分区，并调大最大分区个数
   * @param spark
   */
  def SetMaxPartion(spark:SparkSession){
    //设置动态分区
    spark.sql("set hive.exec.dynamic.partition=true")

    //设置非严格分区，允许所有的分区字段都可以使用动态分区
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    //在所有MR运行的节点上，总共最多可以创建多少个分区
    spark.sql("set hive.exec.max.dynamic.partitions=100000")

    //设置在每个MR运行的节点上最多可以建立创建多少个分区，有可能某个节点上出现了所有分区的数据
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")

    //整个MR job中最多可以创建多少个HDFS文件
    spark.sql("set hive.exec.max.created.files=100000")

  }

  /**
   * 开启压缩
   * @param spark
   */
  def OpenComperssion(spark:SparkSession){
    //设置map输出压缩
    spark.sql("set mapred.output.compress=true")

    //设置Reduce阶段输出压缩，hive最终输出的数据压缩
    spark.sql("set hive.exec.compress.output=true")

  }

  /**
   * 开启动态分区
   * @param spark
   */
  def openDynamicPartition(spark: SparkSession){
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }

  /**
   * 使用Lzo压缩
   * @param spark
   */
  def useLzoCompression(spark: SparkSession):sql.DataFrame = {
    //输入压缩
    spark.sql("set io.compression.codec.lzo.class=com.hadoop.compression.lzo.LzoCodec")

    //设置map输出数据的压缩方式
    spark.sql("set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec")

  }

  /**
   * 使用snappy压缩
   * @param spark
   */
  def useSnappyCompression(spark:SparkSession)={
    spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    //启用Reduce输出压缩
    spark.sql("set mapreduce.output.fileoutputformat.compress=true")
    spark.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
  }

}
