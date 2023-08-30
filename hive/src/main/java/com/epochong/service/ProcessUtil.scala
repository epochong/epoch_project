//package com.iflytek.edu.bigdata.process_tfb.dw
//
//import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
//import com.epochong.service.ProcessTfbBindSchool
//import com.iflytek.edu.bigdata.javaUtil.{DateUtil, ProcessJsonUtil, ProcessTfbBindSchool}
//import org.apache.spark.{HashPartitioner, SparkContext}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//
//import java.text.SimpleDateFormat
//import java.util.Date
//import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
//
//import scala.collection.Map
//import scala.collection.mutable.ArrayBuffer
//import scala.util.parsing.json.JSONArray
//
//object ProcessUtil {
//
//
//  def getValueByKey (jsonObject:JSONObject,key :String) = {
//    if(jsonObject.containsKey(key)){
//      jsonObject.getString(key)
//    }else{
//      "bg-1"
//    }
//  }
//
//  def getSizeByKey (jsonObject:JSONObject,key :String) = {
//    if(jsonObject.containsKey(key)){
//      jsonObject.getJSONArray(key).size().toString
//    }else{
//      "0"
//    }
//  }
//  /**
//    * Created by hshu2 on 2018/10/23.
//    *
//    * @param sc
//    * @param beginDay
//    * @return
//    */
//  def processDwTfbExamArchiveFact (spark: SparkSession, beginDay: String,
//                                   odbZxZtfExam: RDD[((String, String,String, String))]
//                                  ) = {
//    val dw_tfb_exam_archive_fact = odbZxZtfExam.flatMap(x=>{
//      val array =new ArrayBuffer[Row]()
//      val exam_id= x._1
//      val exam_archieve = x._2
//      val create_time = x._3
//      val update_time = x._4
//      if (ProcessJsonUtil.isJsonObjectStr(exam_archieve)) {
//        val jsonObj: JSONObject = JSON.parseObject(exam_archieve)
//        jsonObj.keySet().toArray.mkString(",").split(",")
////        "highFreqErrorBookTopics,myErrorBookTopics,otherErrorBookTopics,regionErrorBookTopics,commonTopics".split(",")
//          .map(error_type=>{
//          val errorBookTopics = jsonObj.getJSONArray(error_type)
//          if(errorBookTopics != null && !errorBookTopics.isEmpty){
//            //for (errorBookTopic: JSONObject <- errorBookTopics) {
//
//            for (i <- 0 until errorBookTopics.size()) {
//              val errorBookTopic=errorBookTopics.getJSONObject(i)
//              //json=>value(String)
//              var topic_id,topic_sort,is_show,source_topic_sort,section_code,section_name,category_code,category_name,is_object,difficulty,source ="bg-1"
//              var similar_topic_num,high_train_topic_num,consolidate_topic_num,foundation_topic_num ="0"
//              var class_avg_score,class_score_rate,score,standard_score,school_avg_score,school_score_rate,user_avg_score ="bg-1"
//              var user_score_rate,area_avg_score,area_score_rate,error_topic_sort,error_analysis ="bg-1"
//
//              topic_id = getValueByKey(errorBookTopic,"id")
//              if(errorBookTopic.containsKey("source")) {
//                source = errorBookTopic.getString("source")
//              }
//              //json=>value(json)=>value(Int)
//              if(errorBookTopic.containsKey("difficulty")) {
//                difficulty = getValueByKey(errorBookTopic.getJSONObject("difficulty"),"value")
//              }
//              //json=>value(json)=>value(String)
//
////              if(errorBookTopic.containsKey("subject")){
////                subject_code = getValueByKey(errorBookTopic.getJSONObject("subject"),"code")
////                subject_name = getValueByKey(errorBookTopic.getJSONObject("subject"),"name")
////              }
////              if(errorBookTopic.containsKey("grade")){
////                if(ProcessJsonUtil.isJsonArrayStr(errorBookTopic.getString("grade")) && errorBookTopic.getJSONArray("grade").size() == 0){
////                  grade_code = "bg-1"
////                }else if(ProcessJsonUtil.isJsonObjectStr(errorBookTopic.getString("grade"))){
////                  grade_code = getValueByKey(errorBookTopic.getJSONObject("grade"),"code")
////                }
////                if(ProcessJsonUtil.isJsonArrayStr(errorBookTopic.getString("grade")) && errorBookTopic.getJSONArray("grade").size() == 0){
////                  grade_name = "bg-1"
////                }else if(ProcessJsonUtil.isJsonObjectStr(errorBookTopic.getString("grade"))){
////                  grade_name = getValueByKey(errorBookTopic.getJSONObject("grade"),"name")
////                }
////              }
////              if(errorBookTopic.containsKey("phase")){
////                 phase_code = getValueByKey(errorBookTopic.getJSONObject("phase"),"code")
////                 phase_name = getValueByKey(errorBookTopic.getJSONObject("phase"),"name")
////              }
//              if(errorBookTopic.containsKey("section")){
//                 section_code = getValueByKey(errorBookTopic.getJSONObject("section"),"code")
//                 section_name = getValueByKey(errorBookTopic.getJSONObject("section"),"name")
//                 category_code = getValueByKey(errorBookTopic.getJSONObject("section"), "categoryCode")
//                 category_name = getValueByKey(errorBookTopic.getJSONObject("section"), "categoryName")
//                if (errorBookTopic.getJSONObject("section").containsKey("isSubjective")){
//                  val is_object_temp = errorBookTopic.getJSONObject("section").getInteger("isSubjective").toString
//                  is_object = if (is_object_temp .equals("1")) {
//                    "0"
//                  }else if (is_object_temp .equals("0")){
//                    "1"
//                  }else {
//                    is_object_temp
//                  }
//                }
//
//              }
//              if(errorBookTopic.containsKey("score")){
//                 class_avg_score = getValueByKey(errorBookTopic.getJSONObject("score"), "classAvgScore")
//                 class_score_rate = getValueByKey(errorBookTopic.getJSONObject("score"), "classScoreRate")
//                 score = getValueByKey(errorBookTopic.getJSONObject("score"), "totalScore")
//                 standard_score = getValueByKey(errorBookTopic.getJSONObject("score"), "standardScore")
//                 school_avg_score = getValueByKey(errorBookTopic.getJSONObject("score"), "schoolAvgScore")
//                 school_score_rate = getValueByKey(errorBookTopic.getJSONObject("score"), "schoolScoreRate")
//                 user_avg_score = getValueByKey(errorBookTopic.getJSONObject("score"), "userAvgScore")
//                 user_score_rate = getValueByKey(errorBookTopic.getJSONObject("score"), "userScoreRate")
//                 area_avg_score = getValueByKey(errorBookTopic.getJSONObject("score"), "cityAvgScore")
//                 area_score_rate = getValueByKey(errorBookTopic.getJSONObject("score"), "cityScoreRate")
//              }
//              //json=>value(array)=>value(string)
//              if(errorBookTopic.containsKey("topicNums")){
//                  topic_sort = errorBookTopic.getJSONArray("topicNums").toArray().mkString(",")
//              }
//              if(errorBookTopic.containsKey("dispTitle")){
//                 source_topic_sort = errorBookTopic.getJSONArray("dispTitle").toArray().mkString(",")
//              }
//              if(errorBookTopic.containsKey("errorTopicNums")){
//                error_topic_sort = errorBookTopic.getJSONArray("errorTopicNums").toArray().mkString(",")
//              }
//              if(errorBookTopic.containsKey("errorAnalysis")){
//                error_analysis = errorBookTopic.getJSONArray("errorAnalysis").toArray().mkString(",")
//              }
//              //json=>value(array)=>value(object)
//              if(errorBookTopic.containsKey("similarTopics")){
//                similar_topic_num = errorBookTopic.getJSONArray("similarTopics").size().toString
//              }
//              if(errorBookTopic.containsKey("highTrainTopics")){
//                high_train_topic_num = errorBookTopic.getJSONArray("highTrainTopics").size().toString
//              }
//              if(errorBookTopic.containsKey("consolidateTopics")){
//                consolidate_topic_num = errorBookTopic.getJSONArray("consolidateTopics").size().toString
//              }
//              if(errorBookTopic.containsKey("foundationTampTopics")){
//                foundation_topic_num = errorBookTopic.getJSONArray("foundationTampTopics").size().toString
//              }
//              //json=>value(boolean)
//              if(errorBookTopic.containsKey("show")){
//                is_show = errorBookTopic.getInteger("show").toString
//              }
//
//              var word_cards_num,phrase_cards_num,knowledge_cards_num,idsent_cards_num,is_show_excellent ="0"
//              if(errorBookTopic.containsKey("cards")){
//                word_cards_num = getSizeByKey(errorBookTopic.getJSONObject("cards"),"wordCards")
//                phrase_cards_num = getSizeByKey(errorBookTopic.getJSONObject("cards"),"phraseCards")
//                knowledge_cards_num = getSizeByKey(errorBookTopic.getJSONObject("cards"),"knowledgeCards")
//                idsent_cards_num = getSizeByKey(errorBookTopic.getJSONObject("cards"),"ldSentCards")
//              }
//
//              if(errorBookTopic.containsKey("writingMaterial")){
//                val excellentAnswerHtml = getValueByKey(errorBookTopic.getJSONObject("writingMaterial"), "excellentAnswerHtml")
//                is_show_excellent = if (excellentAnswerHtml.trim.size>0 && excellentAnswerHtml != "bg-1"){
//                  "1"
//                }else {
//                  "0"
//                }
//              }
//
//              array += Row(exam_id,topic_id,error_type,
//                topic_sort,is_show,source_topic_sort,section_code,section_name,category_code,category_name,is_object,
//                difficulty,source,similar_topic_num,high_train_topic_num,consolidate_topic_num,foundation_topic_num,
//                class_avg_score,class_score_rate,score,standard_score,school_avg_score,school_score_rate,user_avg_score,
//                  user_score_rate,area_avg_score,area_score_rate,error_topic_sort,error_analysis,create_time,update_time,
//                word_cards_num,phrase_cards_num,knowledge_cards_num,idsent_cards_num,is_show_excellent)
//            }
//          }
//        })
//      }
//      array
//    })
//    val str = "exam_id,topic_id,error_type," +
//      "topic_sort,is_show,source_topic_sort,section_code,section_name,category_code,category_name,is_object," +
//      "difficulty,source,similar_topic_num,high_train_topic_num,consolidate_topic_num,foundation_topic_num," +
//      "class_avg_score,class_score_rate,score,standard_score,school_avg_score,school_score_rate,user_avg_score," +
//      "user_score_rate,area_avg_score,area_score_rate,error_topic_sort,error_analysis,create_time,update_time," +
//      "word_cards_num,phrase_cards_num,knowledge_cards_num,idsent_cards_num,is_show_excellent"
//    val schema = Util.getSchemaByColumn(str)
// // val sqlContext = new SQLContext(sc)
//    spark.createDataFrame(dw_tfb_exam_archive_fact, schema)
//  }
//
//  def processDwTfbExamSimilarTopicFact (spark: SparkSession, beginDay: String,
//                                   odbZxZtfExam: RDD[((String, String))]
//                                  ) = {
//    val dw_tfb_exam_similar_topic_fact = odbZxZtfExam.flatMap(x=>{
//      val array =new ArrayBuffer[Row]()
//      val exam_id= x._1
//      val exam_archieve = x._2
//      if (ProcessJsonUtil.isJsonObjectStr(exam_archieve)) {
//        val jsonObj: JSONObject = JSON.parseObject(exam_archieve)
//        jsonObj.keySet().toArray.mkString(",").split(",")
////        "highFreqErrorBookTopics,myErrorBookTopics,otherErrorBookTopics,regionErrorBookTopics,commonTopics".split(",")
//          .map(error_type=>{
//            val errorBookTopics = jsonObj.getJSONArray(error_type)
//            if(errorBookTopics != null && !errorBookTopics.isEmpty){
//              for (i <- 0 until errorBookTopics.size()) {
//                val errorBookTopic=errorBookTopics.getJSONObject(i)
//                //json=>value(String)
//                var topic_id,topic_sort,source_topic_sort,similar_topic_type_temp,section_code,source_topic_type ="bg-1"
//                var similar_topic_id,similar_topic_source,similar_topic_sort,similar_source_topic_sort="bg-1"
//                var difficulty_type,difficulty_code,difficulty_value,difficulty_name,section_name,category_code,category_name,is_object = "bg-1"
//                topic_id = getValueByKey(errorBookTopic,"id")
//
//                //json=>value(array)=>value(string)
//                if(errorBookTopic.containsKey("topicNums")){
//                  topic_sort = errorBookTopic.getJSONArray("topicNums").toArray().mkString(",")
//                }
//                if(errorBookTopic.containsKey("dispTitle")){
//                  source_topic_sort = errorBookTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                }
//                //json=>value(array)=>value(object)
//                if(errorBookTopic.containsKey("similarTopics")){
//                  val similarTopics = errorBookTopic.getJSONArray("similarTopics")
//                  if(similarTopics != null && !similarTopics.isEmpty){
//                    for (i <- 0 until similarTopics.size()) {
//                      val similarTopic=similarTopics.getJSONObject(i)
//                      similar_topic_id = getValueByKey(similarTopic,"id")
//                      if(similarTopic.containsKey("topicNum")) {
//                        similar_topic_sort = similarTopic.getString("topicNum")
//                      }
//                      if(similarTopic.containsKey("dispTitle")){
//                        similar_source_topic_sort = similarTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                      }
//                      if(similarTopic.containsKey("source")) {
//                        similar_topic_source = similarTopic.getString("source")
//                      }
//                      if(similarTopic.containsKey("similarTopicType")) {
//                        similar_topic_type_temp = similarTopic.getString("similarTopicType")
//                      }
//                      if(similarTopic.containsKey("difficultyType")) {
//                        difficulty_type = similarTopic.getString("difficultyType")
//                      }
//                      if(similarTopic.containsKey("sourceTopicType")) {
//                        source_topic_type = similarTopic.getString("sourceTopicType")
//                      }
//                      if(similarTopic.containsKey("section")){
//                        section_code = getValueByKey(similarTopic.getJSONObject("section"),"code")
//                        section_name = getValueByKey(similarTopic.getJSONObject("section"),"name")
//                        category_code = getValueByKey(similarTopic.getJSONObject("section"), "categoryCode")
//                        category_name = getValueByKey(similarTopic.getJSONObject("section"), "categoryName")
//                        if (similarTopic.getJSONObject("section").containsKey("isSubjective")){
//                          val is_object_temp = similarTopic.getJSONObject("section").getInteger("isSubjective").toString
//                          is_object = if (is_object_temp .equals("1")) {
//                            "0"
//                          }else if (is_object_temp .equals("0")){
//                            "1"
//                          }else {
//                            is_object_temp
//                          }
//                        }
//                        }
//                      if(similarTopic.containsKey("difficulty")){
//                        difficulty_code = getValueByKey(similarTopic.getJSONObject("difficulty"), "code")
//                        difficulty_value = getValueByKey(similarTopic.getJSONObject("difficulty"), "value")
//                        difficulty_name = getValueByKey(similarTopic.getJSONObject("difficulty"), "name")
//                      }
//                        val section_code_all = "030400,030423,030500,030522"
//
//                        val similar_topic_type = if (section_code_all.split(",").toList.contains(section_code) && similar_topic_type_temp == "2" ){
//                          "1"
//                        } else {
//                          similar_topic_type_temp
//                      }
//
//                        array += Row(exam_id,topic_id,error_type,topic_sort,source_topic_sort,
//                        similar_topic_id,similar_topic_source,similar_topic_sort,similar_source_topic_sort,
//                          similar_topic_type,difficulty_type,difficulty_code,difficulty_name,difficulty_value,
//                          section_code,section_name,category_code,category_name,is_object,source_topic_type)
//                    }
//                  }
//                }
//              }
//            }
//          })
//      }
//      array
//    })
//    val str = "exam_id,topic_id,error_type,topic_sort,source_topic_sort," +
//      "similar_topic_id,similar_topic_source,similar_topic_sort,similar_source_topic_sort,similar_topic_type," +
//      "difficulty_type,difficulty_code,difficulty_name,difficulty_value,section_code,section_name," +
//      "category_code,category_name,is_object,source_topic_type"
//    val schema = Util.getSchemaByColumn(str)
//    spark.createDataFrame(dw_tfb_exam_similar_topic_fact, schema)
//  }
//
//
//
//  def processDwTfbExamHighTrainTopicFact (spark:SparkSession, beginDay: String,
//                                        odbZxZtfExam: RDD[((String, String))]
//                                       ) = {
//    val dw_tfb_exam_high_train_topic_fact = odbZxZtfExam.flatMap(x=>{
//      val array =new ArrayBuffer[Row]()
//      val exam_id= x._1
//      val exam_archieve = x._2
//      if (ProcessJsonUtil.isJsonObjectStr(exam_archieve)) {
//        val jsonObj: JSONObject = JSON.parseObject(exam_archieve)
//        jsonObj.keySet().toArray.mkString(",").split(",")
////        "highFreqErrorBookTopics,myErrorBookTopics,otherErrorBookTopics,regionErrorBookTopics,commonTopics".split(",")
//          .map(error_type=>{
//            val errorBookTopics = jsonObj.getJSONArray(error_type)
//            if(errorBookTopics != null && !errorBookTopics.isEmpty){
//              for (i <- 0 until errorBookTopics.size()) {
//                val errorBookTopic=errorBookTopics.getJSONObject(i)
//                //json=>value(String)
//                var topic_id,topic_sort,source_topic_sort,section_code,source_topic_type ="bg-1"
//                var high_train_topic_id,high_train_topic_source,high_train_topic_sort,high_train_source_topic_sort="bg-1"
//                var difficulty_type,difficulty_code,difficulty_value,difficulty_name,section_name,category_code,category_name,is_object = "bg-1"
//
//                topic_id = getValueByKey(errorBookTopic,"id")
//
//                //json=>value(array)=>value(string)
//                if(errorBookTopic.containsKey("topicNums")){
//                  topic_sort = errorBookTopic.getJSONArray("topicNums").toArray().mkString(",")
//                }
//                if(errorBookTopic.containsKey("dispTitle")){
//                  source_topic_sort = errorBookTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                }
//                //json=>value(array)=>value(object)
//                if(errorBookTopic.containsKey("highTrainTopics")){
//                  val highTrainTopics = errorBookTopic.getJSONArray("highTrainTopics")
//                  if(highTrainTopics != null && !highTrainTopics.isEmpty){
//                    for (i <- 0 until highTrainTopics.size()) {
//                      val highTrainTopic=highTrainTopics.getJSONObject(i)
//                      high_train_topic_id = getValueByKey(highTrainTopic,"id")
//                      if(highTrainTopic.containsKey("topicNum")) {
//                        high_train_topic_sort = highTrainTopic.getString("topicNum")
//                      }
//                      if(highTrainTopic.containsKey("dispTitle")){
//                        high_train_source_topic_sort = highTrainTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                      }
//                      if(highTrainTopic.containsKey("source")) {
//                        high_train_topic_source = highTrainTopic.getString("source")
//                      }
//
//                      if(highTrainTopic.containsKey("difficultyType")) {
//                        difficulty_type = highTrainTopic.getString("difficultyType")
//                      }
//                      if(highTrainTopic.containsKey("sourceTopicType")) {
//                        source_topic_type = highTrainTopic.getString("sourceTopicType")
//                      }
//                      if(highTrainTopic.containsKey("section")){
//                        section_code = getValueByKey(highTrainTopic.getJSONObject("section"),"code")
//                        section_name = getValueByKey(highTrainTopic.getJSONObject("section"),"name")
//                        category_code = getValueByKey(highTrainTopic.getJSONObject("section"), "categoryCode")
//                        category_name = getValueByKey(highTrainTopic.getJSONObject("section"), "categoryName")
//                        if (highTrainTopic.getJSONObject("section").containsKey("isSubjective")){
//                          val is_object_temp = highTrainTopic.getJSONObject("section").getInteger("isSubjective").toString
//                          is_object = if (is_object_temp .equals("1")) {
//                            "0"
//                          }else if (is_object_temp .equals("0")){
//                            "1"
//                          }else {
//                            is_object_temp
//                          }
//                        }
//                      }
//                      if(highTrainTopic.containsKey("difficulty")){
//                        difficulty_code = getValueByKey(highTrainTopic.getJSONObject("difficulty"), "code")
//                        difficulty_value = getValueByKey(highTrainTopic.getJSONObject("difficulty"), "value")
//                        difficulty_name = getValueByKey(highTrainTopic.getJSONObject("difficulty"), "name")
//                      }
//                      array += Row(exam_id,topic_id,error_type,topic_sort,source_topic_sort,
//                        high_train_topic_id,high_train_topic_source,high_train_topic_sort,high_train_source_topic_sort,
//                          difficulty_type,difficulty_code,difficulty_name,difficulty_value,
//                        section_code,section_name,category_code,category_name,is_object,source_topic_type)
//                    }
//                  }
//                }
//              }
//            }
//          })
//      }
//      array
//    })
//    val str = "exam_id,topic_id,error_type,topic_sort,source_topic_sort," +
//      "high_train_topic_id,high_train_topic_source,high_train_topic_sort,high_train_source_topic_sort," +
//      "difficulty_type,difficulty_code,difficulty_name,difficulty_value,section_code,section_name,category_code," +
//      "category_name,is_object,source_topic_type"
//    val schema = Util.getSchemaByColumn(str)
//    spark.createDataFrame(dw_tfb_exam_high_train_topic_fact, schema)
//  }
//
//
//
//  def processDwTfbExamConsolidateTopicFact (spark: SparkSession, beginDay: String,
//                                          odbZxZtfExam: RDD[((String, String))]
//                                         ) = {
//    val dw_tfb_exam_consolidate_topic_fact = odbZxZtfExam.flatMap(x=>{
//      val array =new ArrayBuffer[Row]()
//      val exam_id= x._1
//      val exam_archieve = x._2
//      if (ProcessJsonUtil.isJsonObjectStr(exam_archieve)) {
//        val jsonObj: JSONObject = JSON.parseObject(exam_archieve)
//        jsonObj.keySet().toArray.mkString(",").split(",")
////        "highFreqErrorBookTopics,myErrorBookTopics,otherErrorBookTopics,regionErrorBookTopics,commonTopics".split(",")
//          .map(error_type=>{
//            val errorBookTopics = jsonObj.getJSONArray(error_type)
//            if(errorBookTopics != null && !errorBookTopics.isEmpty){
//              for (i <- 0 until errorBookTopics.size()) {
//                val errorBookTopic=errorBookTopics.getJSONObject(i)
//                //json=>value(String)
//                var topic_id,topic_sort,source_topic_sort,section_code ="bg-1"
//                var consolidate_topic_id,consolidate_topic_source,consolidate_topic_sort,consolidate_source_topic_sort,source_topic_type="bg-1"
//                var difficulty_type,difficulty_code,difficulty_value,difficulty_name,section_name,category_code,category_name,is_object = "bg-1"
//
//                topic_id = getValueByKey(errorBookTopic,"id")
//
//                //json=>value(array)=>value(string)
//                if(errorBookTopic.containsKey("topicNums")){
//                  topic_sort = errorBookTopic.getJSONArray("topicNums").toArray().mkString(",")
//                }
//                if(errorBookTopic.containsKey("dispTitle")){
//                  source_topic_sort = errorBookTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                }
//                //json=>value(array)=>value(object)
//                if(errorBookTopic.containsKey("consolidateTopics")){
//                  val consolidateTopics = errorBookTopic.getJSONArray("consolidateTopics")
//                  if(consolidateTopics != null && !consolidateTopics.isEmpty){
//                    for (i <- 0 until consolidateTopics.size()) {
//                      val consolidateTopic=consolidateTopics.getJSONObject(i)
//                      consolidate_topic_id = getValueByKey(consolidateTopic,"id")
//                      if(consolidateTopic.containsKey("topicNum")) {
//                        consolidate_topic_sort = consolidateTopic.getString("topicNum")
//                      }
//                      if(consolidateTopic.containsKey("dispTitle")){
//                        consolidate_source_topic_sort = consolidateTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                      }
//                      if(consolidateTopic.containsKey("source")) {
//                        consolidate_topic_source = consolidateTopic.getString("source")
//                      }
//
//                      if(consolidateTopic.containsKey("difficultyType")) {
//                        difficulty_type = consolidateTopic.getString("difficultyType")
//                      }
//                      if(consolidateTopic.containsKey("sourceTopicType")) {
//                        source_topic_type = consolidateTopic.getString("sourceTopicType")
//                      }
//                      if(consolidateTopic.containsKey("section")){
//                        section_code = getValueByKey(consolidateTopic.getJSONObject("section"),"code")
//                        section_name = getValueByKey(consolidateTopic.getJSONObject("section"),"name")
//                        category_code = getValueByKey(consolidateTopic.getJSONObject("section"), "categoryCode")
//                        category_name = getValueByKey(consolidateTopic.getJSONObject("section"), "categoryName")
//                        if (consolidateTopic.getJSONObject("section").containsKey("isSubjective")){
//                          val is_object_temp = consolidateTopic.getJSONObject("section").getInteger("isSubjective").toString
//                          is_object = if (is_object_temp .equals("1")) {
//                            "0"
//                          }else if (is_object_temp .equals("0")){
//                            "1"
//                          }else {
//                            is_object_temp
//                          }
//                        }
//                      }
//                      if(consolidateTopic.containsKey("difficulty")){
//                        difficulty_code = getValueByKey(consolidateTopic.getJSONObject("difficulty"), "code")
//                        difficulty_value = getValueByKey(consolidateTopic.getJSONObject("difficulty"), "value")
//                        difficulty_name = getValueByKey(consolidateTopic.getJSONObject("difficulty"), "name")
//                      }
//                      array += Row(exam_id,topic_id,error_type,topic_sort,source_topic_sort,
//                        consolidate_topic_id,consolidate_topic_source,consolidate_topic_sort,consolidate_source_topic_sort,
//                      difficulty_type,difficulty_code,difficulty_name,difficulty_value,
//                      section_code,section_name,category_code,category_name,is_object,source_topic_type)
//                    }
//                  }
//                }
//              }
//            }
//          })
//      }
//      array
//    })
//    val str = "exam_id,topic_id,error_type,topic_sort,source_topic_sort," +
//      "consolidate_topic_id,consolidate_topic_source,consolidate_topic_sort,consolidate_source_topic_sort," +
//    "difficulty_type,difficulty_code,difficulty_name,difficulty_value,section_code,section_name,category_code,category_name,is_object,source_topic_type"
//    val schema = Util.getSchemaByColumn(str)
//    //val sqlContext = new SQLContext(sc)
//    spark.createDataFrame(dw_tfb_exam_consolidate_topic_fact, schema)
//  }
//
//
//
//
//  def processDwTfbExamFoundationTopicFact (spark:SparkSession, beginDay: String,
//                                            odbZxZtfExam: RDD[((String, String))]
//                                           ) = {
//    val dw_tfb_exam_foundation_topic_fact = odbZxZtfExam.flatMap(x=>{
//      val array =new ArrayBuffer[Row]()
//      val exam_id= x._1
//      val exam_archieve = x._2
//      if (ProcessJsonUtil.isJsonObjectStr(exam_archieve)) {
//        val jsonObj: JSONObject = JSON.parseObject(exam_archieve)
//        jsonObj.keySet().toArray.mkString(",").split(",")
//        //"highFreqErrorBookTopics,myErrorBookTopics,otherErrorBookTopics,regionErrorBookTopics,commonTopics".split(",")
//          .map(error_type=>{
//            val errorBookTopics = jsonObj.getJSONArray(error_type)
//            if(errorBookTopics != null && !errorBookTopics.isEmpty){
//              for (i <- 0 until errorBookTopics.size()) {
//                val errorBookTopic=errorBookTopics.getJSONObject(i)
//                //json=>value(String)
//                var topic_id,topic_sort,source_topic_sort,section_code,source_topic_type ="bg-1"
//                var foundation_topic_id,foundation_topic_source,foundation_topic_sort,foundation_source_topic_sort="bg-1"
//                var difficulty_type,difficulty_code,difficulty_value,difficulty_name,section_name,category_code,category_name,is_object = "bg-1"
//
//                topic_id = getValueByKey(errorBookTopic,"id")
//
//                //json=>value(array)=>value(string)
//                if(errorBookTopic.containsKey("topicNums")){
//                  topic_sort = errorBookTopic.getJSONArray("topicNums").toArray().mkString(",")
//                }
//                if(errorBookTopic.containsKey("dispTitle")){
//                  source_topic_sort = errorBookTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                }
//                //json=>value(array)=>value(object)
//                if(errorBookTopic.containsKey("foundationTampTopics")){
//                  val foundationTampTopics = errorBookTopic.getJSONArray("foundationTampTopics")
//                  if(foundationTampTopics != null && !foundationTampTopics.isEmpty){
//                    for (i <- 0 until foundationTampTopics.size()) {
//                      val foundationTampTopic=foundationTampTopics.getJSONObject(i)
//                      foundation_topic_id = getValueByKey(foundationTampTopic,"id")
//                      if(foundationTampTopic.containsKey("topicNum")) {
//                        foundation_topic_sort = foundationTampTopic.getString("topicNum")
//                      }
//                      if(foundationTampTopic.containsKey("dispTitle")){
//                        foundation_source_topic_sort = foundationTampTopic.getJSONArray("dispTitle").toArray().mkString(",")
//                      }
//                      if(foundationTampTopic.containsKey("source")) {
//                        foundation_topic_source = foundationTampTopic.getString("source")
//                      }
//
//                      if(foundationTampTopic.containsKey("difficultyType")) {
//                        difficulty_type = foundationTampTopic.getString("difficultyType")
//                      }
//                      if(foundationTampTopic.containsKey("sourceTopicType")) {
//                        source_topic_type = foundationTampTopic.getString("sourceTopicType")
//                      }
//                      if(foundationTampTopic.containsKey("section")){
//                        section_code = getValueByKey(foundationTampTopic.getJSONObject("section"),"code")
//                        section_name = getValueByKey(foundationTampTopic.getJSONObject("section"),"name")
//                        category_code = getValueByKey(foundationTampTopic.getJSONObject("section"), "categoryCode")
//                        category_name = getValueByKey(foundationTampTopic.getJSONObject("section"), "categoryName")
//                        if (foundationTampTopic.getJSONObject("section").containsKey("isSubjective")){
//                          val is_object_temp = foundationTampTopic.getJSONObject("section").getInteger("isSubjective").toString
//                          is_object = if (is_object_temp .equals("1")) {
//                            "0"
//                          }else if (is_object_temp .equals("0")){
//                            "1"
//                          }else {
//                            is_object_temp
//                          }
//                        }
//                      }
//                      if(foundationTampTopic.containsKey("difficulty")){
//                        difficulty_code = getValueByKey(foundationTampTopic.getJSONObject("difficulty"), "code")
//                        difficulty_value = getValueByKey(foundationTampTopic.getJSONObject("difficulty"), "value")
//                        difficulty_name = getValueByKey(foundationTampTopic.getJSONObject("difficulty"), "name")
//                      }
//                      array += Row(exam_id,topic_id,error_type,topic_sort,source_topic_sort,
//                        foundation_topic_id,foundation_topic_source,foundation_topic_sort,foundation_source_topic_sort,
//                        difficulty_type,difficulty_code,difficulty_name,difficulty_value,
//                        section_code,section_name,category_code,category_name,is_object,source_topic_type)
//                    }
//                  }
//                }
//              }
//            }
//          })
//      }
//      array
//    })
//    val str = "exam_id,topic_id,error_type,topic_sort,source_topic_sort," +
//      "foundation_topic_id,foundation_topic_source,foundation_topic_sort,foundation_source_topic_sort," +
//    "difficulty_type,difficulty_code,difficulty_name,difficulty_value,section_code,section_name,category_code," +
//      "category_name,is_object,source_topic_type"
//    val schema = Util.getSchemaByColumn(str)
//    spark.createDataFrame(dw_tfb_exam_foundation_topic_fact, schema)
//  }
//
//  def processDwTfbStudentAnswerDetailFact (spark: SparkSession, beginDay: String, OdbZxZtfStudentAnswerDetail: RDD[((String, String),(String,String,String))]) = {
//
//    OdbZxZtfStudentAnswerDetail.flatMap(x=>{
//      val array = new ArrayBuffer[((String, String,String, String, String,String, String, String,
//        String,String, String, String, String, String))]()
//      val exam_id=x._1._1
//      val student=x._1._2
//      val topicanswerdetails=x._2._1
//      val create_time=x._2._2
//      val update_time=x._2._3
//      var student_id ,student_name ,student_code,topic_id,topic_sort,topic_answer,is_fill = "bg-1"
//      var similar_topic_id,similar_topic_sort,similar_topic_answer,similar_is_fill = "bg-1"
//
//      if (ProcessJsonUtil.isJsonObjectStr(student)){
//        student_id = ProcessJsonUtil.parseJsonObject(student, "id")
//        student_name = ProcessJsonUtil.parseJsonObject(student, "name")
//        student_code = ProcessJsonUtil.parseJsonObject(student, "code")
//      }
//      if (ProcessJsonUtil.isJsonArrayStr(topicanswerdetails) ){
//        val a= ProcessJsonUtil.parseJsonArray(topicanswerdetails, "topicNum,topicId,topicAnswerResult,isFill,similarTopicAnswerDetail")
//        for (i <- 0 until a.size()) {
//          val s = a.get(i).split("\t", -1)
//          topic_sort = Util.json_to_bg1(s(0))
//          topic_id = Util.json_to_bg1(s(1))
//          topic_answer = Util.json_to_bg1(s(2))
//          is_fill = if (s(3).toString == "true") "1" else "0"
//          val similarTopicAnswerDetail = Util.to_bg1(s(4))
//          if (ProcessJsonUtil.isJsonArrayStr(similarTopicAnswerDetail) ) {
//            val b = ProcessJsonUtil.parseJsonArray(similarTopicAnswerDetail, "topicNum,topicId,topicAnswerResult,isFill")
//            if (b.size > 0) {
//              for (i <- 0 until b.size()) {
//                val s = b.get(i).split("\t", -1)
//                similar_topic_sort = Util.json_to_bg1(s(0))
//                similar_topic_id = Util.json_to_bg1(s(1))
//                similar_topic_answer = Util.json_to_bg1(s(2))
//                similar_is_fill = if (s(3).toString == "true") "1" else "0"
//                array += ((exam_id, student_id, student_name, student_code, topic_id, topic_sort, topic_answer, is_fill,
//                  similar_topic_id, similar_topic_sort, similar_topic_answer, similar_is_fill, create_time, update_time))
//              }
//            } else {
//              array += ((exam_id, student_id, student_name, student_code, topic_id, topic_sort, topic_answer, is_fill,
//                similar_topic_id, similar_topic_sort, similar_topic_answer, similar_is_fill, create_time, update_time))
//            }
//          }
//        }
//      }else{
//        array+=((	exam_id,student_id,student_name,student_code,topic_id,topic_sort,topic_answer,is_fill,
//          similar_topic_id,similar_topic_sort,similar_topic_answer,similar_is_fill,create_time,update_time))
//      }
//      array
//    })
//
//  }
//
//  /**
//    * Created by ddjia on 2023/08/25
//    *
//    * @param sc
//    * @param beginDay
//    * @return
//    */
//  def processDwTfbBindSchool (spark: SparkSession, beginDay: String, bindSchoolRdd: RDD[(String, String)]) = {
//
//      val map_new = bindSchoolRdd.flatMap(x => {
//      val array =new ArrayBuffer[Row]()
//      val school_id = x._1.toString
//      val retain_school_id = x._2.toString
//      ProcessTfbBindSchool.setValue(school_id, retain_school_id)
//      val list = ProcessTfbBindSchool.dealDatas()
//      if (list.size > 0) {
//        for (i <- 0 until list.size()) {
//          array += Row(list.get(i).get(0), list.get(i).get(1))
//        }
//      }
//      array
//    })
//    val str = "school_id,retain_school_id"
//    val schema = Util.getSchemaByColumn(str)
//    spark.createDataFrame(map_new, schema)
//  }
//}
//
//
//
//
