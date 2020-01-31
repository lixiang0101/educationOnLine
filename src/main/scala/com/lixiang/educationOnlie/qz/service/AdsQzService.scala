package com.lixiang.educationOnlie.qz.service

import com.lixiang.educationOnlie.qz.dao.AdsQzDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AdsQzService {
  def getTarget(spark:SparkSession,dt:String){
    val avgDetail = AdsQzDao.getAvgSpendTimeAndScore(spark, dt)
    val topscore = AdsQzDao.getTopScore(spark, dt)
    val top3UserDetail = AdsQzDao.getTop3UserDetial(spark, dt)
    val low3UserDetail = AdsQzDao.getLow3UserDetail(spark, dt)
    val paperScore = AdsQzDao.getPaperScoreSegmentUser(spark, dt)
    val paperPassDetail = AdsQzDao.getPaperPassDetail(spark, dt)
    val questionDetail = AdsQzDao.getQuestionDetail(spark, dt)
  }
  def getTargetApi(spark:SparkSession,dt:String){
    import org.apache.spark.sql.functions._
    val avgDetail = spark.sql("select paperviewid,paperviewname,score,spendtime,dt,dn from dws.dws_user_paper_detail ")
      .where(s"dt='$dt'").groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(avg("score").cast("decimal(4,2)").as("avgscore"),
        avg("spendtime").cast("decimal(4,2)").as("avgspendtime"))
      .select("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_avgtimeandscore")

    val topScore: Unit = spark.sql("select paperviewid,paperviewname,score dt,dn from dws.dwr_paper_des_usetail")
      .where(s"dt='$dt'").groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(max("score").as("maxscore"), min("score").as("minscore"))
      .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")

    val top3Score: Unit = spark.sql("select * from dws.dwr_paper_des_usetail")
      .where(s"dt='$dt'").select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
      , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
      .where("rk < 4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")

    val low3UserDetail = spark.sql("select * from dws.dwr_paper_des_usetail")
      .where(s"dt='$dt'").select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
      , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy("score")))
      .where("rk < 4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")

    val paperScore: Unit = spark.sql("select *from dws.dws_user_paper_detail")
      .where(s"dt='$dt'").select("paperviewid", "paperviewname", "userid", "score", "dt", "dn")
      .withColumn("score_segment",
        when(col("score").between(0, 20), "0-20")
          .when(col("score") > 20 && col("score") <= 40, "20-40")
          .when(col("score") > 40 && col("score") <= 60, "40-60")
          .when(col("score") > 60 && col("score") <= 80, "60-80")
          .when(col("score") > 80 && col("score") <= 100, "80-100"))
      .drop("score").groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
      .agg(concat_ws(",", collect_list(col("userid").cast("string").as("userids"))))
      .select("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
      .orderBy("paperviewid", "score_segment")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")

    val paperPassDetail: DataFrame = spark.sql("select *from dws.dws_user_paper_detail").cache()
    val unPassDetail: DataFrame = paperPassDetail.select("paperviewid", "paperviewname", "dn", "dt")
      .where(s"dt='$dt'").where("score < 60")
      .groupBy("paperviewid", "paperviewname", "dn", "dt")
      .agg(count("paperviewid").as("unpasscount"))

    val passDetail: DataFrame = paperPassDetail.select("paperviewid", "paperviewname", "dn", "dt")
      .where(s"dt='$dt'").where("score >= 60")
      .groupBy("paperviewid", "paperviewname", "dn", "dt")
      .agg(count("paperviewid").as("passcount"))

    passDetail.join(unPassDetail,Seq("paperviewid","dt"))
      .withColumn("rate",(col("passcount")./(col("passcount")+col("unpasscount"))).cast("decimal(4,2)"))
      .select("paperviewid","paperviewname", "unpasscount", "passcount", "rate", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")

    paperPassDetail.unpersist()//释放cache

    val userQuestionDetail = spark.sql("select * from dws.dws_user_paper_detail").cache()
    val userQuestionError: DataFrame = userQuestionDetail.select("questionid", "dt", "dn", "user_question_answer")
      .where(s"dt='$dt'").where("user_question_answer='0'").drop("user_question_answer")
      .groupBy("questionid", "dt", "dn")
      .agg(count("questionid").as("errorcount"))
    val userQuestionRight: DataFrame = userQuestionDetail.select("questionid", "dt", "dn", "user_question_answer")
      .where(s"dt='$dt'").where("user_question_answer='1'").drop("user_question_answer")
      .groupBy("questionid", "dt", "dn")
      .agg(count("questionid").as("rightcount"))

    userQuestionError.join(userQuestionRight,Seq("questionid","dt","dn"))
      .withColumn("rate",(col("rightcount")./(col("rightcount")+col("errorcount"))).cast("decimal(4,2)"))
      .select("questionid","errorcount","rightcount","rate","dt","dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_question_detail")
  }
}
