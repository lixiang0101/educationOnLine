package com.lixiang.educationOnlie.qz.service

import com.lixiang.educationOnlie.qz.dao.{QzChapterDao, QzCourseDao, QzMajorDao, QzPaperDao, QzQuestionDao, UserPaperDetailDao}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 维度退化、合成宽表 业务类
 */
object DwsQzService {
  def saveDwsQzChapter(spark:SparkSession,dt:String){
    val dwdQzChapter: DataFrame = QzChapterDao.getDwdQzChapter(spark,dt)
    val dwdQzChapterList: DataFrame = QzChapterDao.getDwdQzChapterList(spark,dt)
    val dwdQzPoint: DataFrame = QzChapterDao.getDwdQzPoint(spark,dt)
    val dwdQzPointQuestion = QzChapterDao.getDwdQzPointQuestion(spark, dt)
    val result: DataFrame = dwdQzChapter.join(dwdQzChapterList, Seq("chapterlistid", "dn"))
      .join(dwdQzPoint, Seq("chapterid", "dn"))
      .join(dwdQzPointQuestion, Seq("pointid", "dn"))
    result.select("chapterid","chapterlistid", "chaptername", "sequence", "showstatus","showstatus",
      "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
      "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
      "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
  }
  def saveDwsQzCourse(spark:SparkSession,dt:String){
    val dwdQzSiteCourse = QzCourseDao.getDwdQzSiteCourse(spark, dt)
    val dwdQzCourse = QzCourseDao.getDwdQzCourse(spark, dt)
    val dwdQzCourseEdusubject = QzCourseDao.getDwdQzCourseEduSubject(spark, dt)
    val result: DataFrame = dwdQzSiteCourse.join(dwdQzCourse, Seq("courseid", "dn"))
      .join(dwdQzCourseEdusubject, Seq("courseid", "dn"))
      .select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
        "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
        "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
        , "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
  }

  def saveDwsQzMajor(spark:SparkSession,dt:String){
    val dwdQzMajor = QzMajorDao.getQzMajor(spark, dt)
    val dwdQzWebsite = QzMajorDao.getQzWebsite(spark, dt)
    val dwdQzBusiness = QzMajorDao.getQzBusiness(spark, dt)
    val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
      .join(dwdQzBusiness, Seq("businessid", "dn"))
      .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
        "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
        "multicastgateway", "multicastport", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
  }

  def saveDwsQzPaper(spark: SparkSession, dt: String) = {
    val dwdQzPaperView = QzPaperDao.getDwdQzPaperView(spark, dt)
    val dwdQzCenterPaper = QzPaperDao.getDwdQzCenterPaper(spark, dt)
    val dwdQzCenter = QzPaperDao.getDwdQzCenter(spark, dt)
    val dwdQzPaper = QzPaperDao.getDwdQzPaper(spark, dt)
    val result = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))
      .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
        , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
        "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
        "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
        "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
  }

  def saveDwsQzQuestionTpe(spark: SparkSession, dt: String) = {
    val dwdQzQuestion = QzQuestionDao.getQzQuestion(spark, dt)
    val dwdQzQuestionType = QzQuestionDao.getQzQuestionType(spark, dt)
    val result = dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
      .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
        , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
        , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
        "remark", "splitscoretype", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")
  }

  def saveDwsUserPaperDetail(spark:SparkSession,dt:String): Unit ={
      val dwdQzMemberPaperQuestion: DataFrame = UserPaperDetailDao.getDwdQzMemberPaperQuestion(spark,dt).drop("paperid")
        .withColumnRenamed("question_answer","user_question_answer")
    val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(spark, dt).drop("courseid")
    val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(spark, dt).withColumnRenamed("sitecourse_creator", "course_creator")
      .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
      .drop("chapterlistid").drop("pointlistid")
    val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(spark, dt)
    val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(spark, dt).drop("courseid")
    val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(spark, dt)
    dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
      join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
      .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")
  }
}
