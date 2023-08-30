//package com.iflytek.edu.bigdata.process_tfb.dw
//
//import org.apache.spark.sql.{SaveMode, SparkSession}
//
//object DwTfbBindSchool {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .config("spark.hadoop.validateOutputSpecs", "false")
//      .config("spark.sql.parquet.compression.codec", "gzip")
//      .config("spark.app.name", this.getClass.getName)
//      .enableHiveSupport()
//      .getOrCreate()
//    //获取天
//    val beginDay = Util.getBeginDay(args)
//    //数据输出路径
//    val write_path = Util.getValuePath(args) + Conf.dw_tfb_govern_bind_school + beginDay
//    //读取数据并处理生成新的表
//    process(spark, beginDay, args).write.format("parquet").mode(SaveMode.Overwrite).save(write_path)
//    spark.stop()
//
//  }
//  def process(spark: SparkSession, beginDay: String, args: Array[String]) = {
//    // 读取应用使用数
//    val bindSchoolRdd = ReadUtil.readOdbTfbGovernBindSchool(spark, beginDay)
//
//    ProcessUtil.processDwTfbBindSchool(spark, beginDay, bindSchoolRdd).repartition(1)
//  }
//}
