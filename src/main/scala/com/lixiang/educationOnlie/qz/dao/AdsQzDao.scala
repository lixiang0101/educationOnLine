package com.lixiang.educationOnlie.qz.dao

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
 * 报表层各指标统计
 * ADS(APP/DAL/DF)-出报表结果 Application Data Store
 */

object AdsQzDao {
  /**
   * 试卷平均分和平均时间
   * @param spark
   * @param dt
   */
  def getAvgSpendTimeAndScore(spark:SparkSession,dt:String){
    spark.sql("select paperviewid,paperviewname,cast(avg(score) as decimal(4,2)),cast(avg(spendtime) as decimal(4,2)),spendtime,dt,dn " +
      "from dws.dws_user_paper_detail " +
      s"where dt = '$dt'" +
      "group by paperviewid,paperviewname,dt,dn " +
      "order by score desc,spendtime desc")
  }

  /**
   * 统计试卷 最高分 最低分
   * @param spark
   * @param dt
   */
  def getTopScore(spark: SparkSession, dt: String) = {
    spark.sql("select paperviewid,paperviewname,cast(max(score) as decimal(4,1)),cast(min(score) as decimal(4,1)) " +
      s",dt,dn from dws.dwr_paper_des_usetail where dt=$dt group by paperviewid,paperviewname,dt,dn ")
  }

  /**
   * 统计前3名
   * @param spark
   * @param dt
   */
  def getTop3UserDetial(spark:SparkSession,dt:String): Unit ={
    spark.sql("select * from " +
      "(select userid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname,sitename,papername,score," +
      "dese_rank() over(partition by paperviewid order by score desc)as rk,dt,dn " +
      " from dws.dws_user_paper_detail)" +
      "where rk < 4")
  }

  /**
   * 按试卷分组获取每份试卷的分数倒数三的用户详情
   * @param spark
   * @param dt
   * @return
   */
  def getLow3UserDetail(spark: SparkSession, dt: String) = {
    spark.sql("select *from (select userid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname," +
      s"sitename,papername,score,dense_rank() over (partition by paperviewid order by score asc) as rk,dt,dn from dws.dws_user_paper_detail where dt='$dt') where rk<4")
  }

 def getPaperScoreSegmentUser(spark: SparkSession, dt: String): sql.DataFrame ={
    spark.sql("select paperviewid,paperviewname,score_segment, concat_ws(',',collect_list(cast(userid as string))),dt,dn " +
      "from (select from paperviewid,paperviewname,userid," +
      "case when score > 80 then '80-100' " +
      "when score > 60 then '60-80' " +
      "when score > 40 then '40-60' " +
      "when score > 20 then '20-40' " +
      "when score > 0 then '0-20' end as score_segment " +
      s"from dws.dws_user_paper_detail where dt='$dt') " +
      "group by paperviewid,paperviewname,score_segment,dt,dn order by paperviewid,score_segment")
 }

  def getPaperPassDetail(spark:SparkSession,dt:String){
    spark.sql("select t.*,cast(t.passcount/(t.passcount+t.countdetail) as decimal(4,2)) as rate,dt,dn from " +
      s"((select paperviewid,paperviewname,count(*) countdetail,dt,dn from dws.dws_user_paper_detail where dt='$dt' and score between 0 and 60 group by paperviewid,paperviewname,dt,dn) a" +
      s"join (select paperviewid,paperviewname,count(*) passcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and score >= 60 group by paperviewid,paperviewname,dt,dn) b on  a.paperviewid=b.paperviewid and a.dn=b.dn) t" +
      "")
  }

  /**
   * 统计各题 正确人数 错误人数 错题率 top3错误题数多的questionid
   * @param spark
   * @param dt
   */
  def getQuestionDetail(spark: SparkSession, dt: String) = {
    spark.sql(s"select t.*,cast(t.errcount/(t.errcount+t.rightcount) as decimal(4,2))as rate" +
      s" from((select questionid,count(*) errcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='0' " +
      s"group by questionid,dt,dn) a join(select questionid,count(*) rightcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='1' " +
      s"group by questionid,dt,dn) b on a.questionid=b.questionid and a.dn=b.dn)t order by errcount desc")
  }
}
