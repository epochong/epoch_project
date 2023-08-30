//package com.iflytek.edu.bigdata.process_tfb.dw
//
//import com.iflytek.edu.bigdata.javaUtil.DateUtil
//import com.iflytek.edu.bigdata.scalaUtil.PublicUtil
//import org.apache.spark.sql.{SQLContext, SparkSession}
//import org.apache.spark.{HashPartitioner, SparkContext}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by hshu2 on 2019/4/11.
//  */
//
//object ReadUtil {
//
//
//  /** *
//    *
//    * @param sc
//    * @param beginDay
//    */
//  def readParquetFile(sparkSession: SparkSession, file_path: String, col_list: List[String]) = {
//    sparkSession.read.parquet(file_path).select(col_list.head, col_list.tail:_*)
//  }
//  def readOdbZxZtfExam(spark: SparkSession, beginDay: String) = {
//    // 获取路径
//    val odb_zx_ztf_exam = Conf.odb_zx_ztf_exam_new
//    //    val sqlContext = new SQLContext(sc)
//    //    import sqlContext.implicits._
//    val df = spark.read.parquet(odb_zx_ztf_exam)
//    df.createOrReplaceTempView("odb_zx_ztf_exam")
//
//    spark.sql("select examid,examarchieve,inserttime,updatetime " +
//      " from odb_zx_ztf_exam").rdd.map(x => {
//      val exam_id = Util.to_bg1(x(0))
//      val exam_archieve = Util.to_bg1(x(1))
//      val create_time = Util.to_bg1(x(2))
//      val update_time = Util.to_bg1(x(3))
//      (exam_id, exam_archieve, create_time, update_time)
//    })
//  }
//
//  def readOdbTfbGovernBindSchool(spark: SparkSession, beginDay: String) = {
//    // 获取路径
//    val odb_tfb_bind_school = Conf.odb_ztf_yygl_sp_school_process_task + beginDay + "/"
//    val df = spark.read.parquet(odb_tfb_bind_school)
//    df.createOrReplaceTempView("odb_tfb_bind_school")
//
//    spark.sql("select task_id,source_school_id,target_school_id,insert_time,update_time " +
//      " from odb_tfb_bind_school where status ='0' and type = '1' order by update_time").rdd.map(x => {
//      val task_id = Util.to_bg1(x(0))
//      val school_id = Util.to_bg1(x(1))
//      val retain_school_id = Util.to_bg1(x(2))
//      ((school_id, retain_school_id))
//    })
//  }
//
//  def readOdbZxZtfExam2(spark: SparkSession, beginDay: String) = {
//    // 获取路径
//    val odb_zx_ztf_exam = Conf.odb_zx_ztf_exam_new
//    val df = spark.read.parquet(odb_zx_ztf_exam)
//    df.createOrReplaceTempView("odb_zx_ztf_exam")
//
//    spark.sql("select examid,examarchieve " +
//      " from odb_zx_ztf_exam").rdd.map(x => {
//      val exam_id = Util.to_bg1(x(0))
//      val exam_archieve = Util.to_bg1(x(1))
//      (exam_id, exam_archieve)
//    })
//  }
//
//
//  def readOdbZxZtfStudentAnswerDetail(spark: SparkSession, beginDay: String) = {
//    // 获取路径
//    val odb_zx_ztf_student_answer_detail = Conf.odb_zx_ztf_student_answer_detail + beginDay
//    val df = spark.read.parquet(odb_zx_ztf_student_answer_detail)
//    df.createOrReplaceTempView("odb_zx_ztf_student_answer_detail")
//
//    spark.sql("select examid,student,topicanswerdetails,inserttime,updatetime " +
//      " from odb_zx_ztf_student_answer_detail").rdd.map(x => {
//      val exam_id = Util.to_bg1(x(0))
//      val student = Util.to_bg1(x(1))
//      val topicanswerdetails = Util.to_bg1(x(2))
//      val inserttime = Util.to_bg1(x(3))
//      val updatetime = Util.to_bg1(x(4))
//      ((exam_id, student), (topicanswerdetails, inserttime,updatetime))
//    }).reduceByKey({ case (a, b) => {
//      if (a._2 > b._2) {
//        a
//      } else {
//        b
//      }
//    }
//    })
//  }
//}
//
//
//
//
