package com.lixiang.educationOnlie.member.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lixiang.educationOnlie.member.bean.Models.BaseAdLog
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EtlDataService {
  /**
   *
   * @param ssc
   * @param spark
   */
  def etlMemberRegtypeLog(ssc: SparkContext, spark: SparkSession) {
    import spark.implicits._
    val dataRDD: RDD[String] = ssc.textFile("hdfs://cmaster.basichadoop.com:8020/user/atguigu/ods/memberRegtype.log")
    //过滤出JSON格式的数据
    val JSONDataRDD: RDD[String] = dataRDD.filter(item => {
      val jsonData = JSON.parseObject(item)
      jsonData.isInstanceOf[JSONObject]
    })

    val tupleRDD: RDD[(Int, String, String, String, String, String, String, String, String, Int, String, String)] =
      JSONDataRDD.mapPartitions(partition => {
        partition.map(item => {
          val jsonObject: JSONObject = JSON.parseObject(item)
          val appkey = jsonObject.getString("appkey").trim
          val appregurl = jsonObject.getString("appregurl").trim
          val bdp_uuid = jsonObject.getString("bdp_uuid").trim
          val createtime = jsonObject.getString("createtime").trim
          val domain: String = jsonObject.getString("domain").trim
          val isranreg = jsonObject.getString("isranreg").trim
          val regsource = jsonObject.getString("regsource").trim
          val regsourceName = regsource match {
            case "1" => "PC"
            case "2" => "Mobile"
            case "3" => "App"
            case "4" => "WeChat"
            case _ => "other"
          }
          val uid = jsonObject.getIntValue("uid")
          val websiteid = jsonObject.getIntValue("websiteid")
          val dt = jsonObject.getString("dt").trim
          val dn = jsonObject.getString("dn").trim
          (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, dt, dn)
        })
      })
    val df: DataFrame = tupleRDD.toDF()
    df.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_member_regtype")
  }

  /**
   *
   * @param ssc
   * @param spark
   */
  def etlBaseAdLog(ssc: SparkContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val rdd: RDD[String] = ssc.textFile("hdfs://cmaster.basichadoop.com:8020/user/atguigu/ods/baseadlog.log")
    val unit: RDD[BaseAdLog] = rdd.filter { item =>
      val obj: JSONObject = JSON.parseObject(item)
      obj.isInstanceOf[JSONObject]
    }.mapPartitions(partition => {
      partition.map(item => {
        val obj: JSONObject = JSON.parseObject(item)
        val adid: Int = obj.getIntValue("adid")
        val adname: String = obj.getString("adname").trim
        val dn: String = obj.getString("dn").trim
        BaseAdLog(adid, adname, dn)
      })
    })
    val ds = unit.toDF()
    ds.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
  }

  def etlMemberLog(ssc:SparkContext,spark:SparkSession): Unit ={
    import spark.implicits._
    val rdd: RDD[String] = ssc.textFile("hdfs://cmaster.basichadoop.com:8020/user/atguigu/ods/member.log")
    val dataRDD = rdd.filter(item => {
      val obj: JSONObject = JSON.parseObject(item)
      obj.isInstanceOf[JSON]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject: JSONObject = JSON.parseObject(item)
        val ad_id = jsonObject.getIntValue("ad_id")
        val birthday = jsonObject.getString("birthday")
        val email = jsonObject.getString("email")
        val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
        val iconurl = jsonObject.getString("iconurl")
        val lastlogin = jsonObject.getString("lastlogin")
        val mailaddr = jsonObject.getString("mailaddr")
        val memberlevel = jsonObject.getString("memberlevel")
        val password = "******"
        val paymoney = jsonObject.getString("paymoney")
        val phone = jsonObject.getString("phone")
        val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
        val qq = jsonObject.getString("qq")
        val register = jsonObject.getString("register")
        val regupdatetime = jsonObject.getString("regupdatetime")
        val uid = jsonObject.getIntValue("uid")
        val unitname = jsonObject.getString("unitname")
        val userip = jsonObject.getString("userip")
        val zipcode = jsonObject.getString("zipcode")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
          register, regupdatetime, unitname, userip, zipcode, dt, dn)
      })
    })
    dataRDD.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_member")
  }

  def etlBaseWebSiteLog(ssc: SparkContext, spark: SparkSession) = {
    import spark.implicits._ //隐式转换
    ssc.textFile("hdfs://cmaster.basichadoop.com:8020/user/atguigu/ods/baswewebsite.log").filter(item => {
      val obj = JSON.parseObject(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = JSON.parseObject(item)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val siteurl = jsonObject.getString("siteurl")
        val delete = jsonObject.getIntValue("delete")
        val createtime = jsonObject.getString("createtime")
        val creator = jsonObject.getString("creator")
        val dn = jsonObject.getString("dn")
        (siteid, sitename, siteurl, delete, createtime, creator, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }

  def etlMemPayMoneyLog(ssc: SparkContext, spark: SparkSession) = {
    import spark.implicits._ //隐式转换
    ssc.textFile("hdfs://cmaster.basichadoop.com:8020/user/atguigu/ods/pcentermempaymoney.log").filter(item => {
      val obj = JSON.parseObject(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = JSON.parseObject(item)
        val paymoney = jSONObject.getString("paymoney")
        val uid = jSONObject.getIntValue("uid")
        val vip_id = jSONObject.getIntValue("vip_id")
        val site_id = jSONObject.getIntValue("siteid")
        val dt = jSONObject.getString("dt")
        val dn = jSONObject.getString("dn")
        (uid, paymoney, site_id, vip_id, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
  }

  def etlMemVipLevelLog(ssc: SparkContext, spark: SparkSession) = {
    import spark.implicits._ //隐式转换
    ssc.textFile("hdfs://cmaster.basichadoop.com:8020/user/atguigu/ods/pcenterMemViplevel.log").filter(item => {
      val obj = JSON.parseObject(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = JSON.parseObject(item)
        val discountval = jSONObject.getString("discountval")
        val end_time = jSONObject.getString("end_time")
        val last_modify_time = jSONObject.getString("last_modify_time")
        val max_free = jSONObject.getString("max_free")
        val min_free = jSONObject.getString("min_free")
        val next_level = jSONObject.getString("next_level")
        val operator = jSONObject.getString("operator")
        val start_time = jSONObject.getString("start_time")
        val vip_id = jSONObject.getIntValue("vip_id")
        val vip_level = jSONObject.getString("vip_level")
        val dn = jSONObject.getString("dn")
        (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
  }
}
