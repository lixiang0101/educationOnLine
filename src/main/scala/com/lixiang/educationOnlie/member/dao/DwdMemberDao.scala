package com.lixiang.educationOnlie.member.dao

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object DwdMemberDao {
  def getDwdMember(spark:SparkSession):sql.DataFrame = {
    spark.sql("select uid,ad_id,email,fullname,iconurl,lastlogin,mailaddr,memberlevel," +
      "password,phone,qq,register,regupdatetime,unitname,userip,zipcode,dt,dn from dwd.dwd_member")
  }

  def getDwdMemberRegType(spark: SparkSession) = {
    spark.sql("select uid,appkey,appregurl,bdp_uuid,createtime as reg_createtime,domain,isranreg," +
      "regsource,regsourcename,websiteid as siteid,dn from dwd.dwd_member_regtype ")
  }

  def getDwdBaseAd(spark: SparkSession) = {
    spark.sql("select adid as ad_id,adname,dn from dwd.dwd_base_ad")
  }

  def getDwdBaseWebSite(spark: SparkSession) = {
    spark.sql("select siteid,sitename,siteurl,delete as site_delete," +
      "createtime as site_createtime,creator as site_creator,dn from dwd.dwd_base_website")
  }

  def getDwdVipLevel(spark: SparkSession) = {
    spark.sql("select vip_id,vip_level,start_time as vip_start_time,end_time as vip_end_time," +
      "last_modify_time as vip_last_modify_time,max_free as vip_max_free,min_free as vip_min_free," +
      "next_level as vip_next_level,operator as vip_operator,dn from dwd.dwd_vip_level")
  }

  def getDwdPcentermemPayMoney(spark: SparkSession) = {
    spark.sql("select uid,cast(paymoney as decimal(10,4)) as paymoney,vip_id,dn from dwd.dwd_pcentermempaymoney")
  }
}
