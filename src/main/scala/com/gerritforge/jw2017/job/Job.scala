package com.gerritforge.jw2017.job

import com.gerritforge.jw2017.model.Event
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.gerritforge.jw2017.etl.Readers._
import com.gerritforge.jw2017.etl.Transformers._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait Job {

  def loadTextFiles(inputDir: String)(implicit sc: SparkSession): RDD[String] = sc.sparkContext.textFile(s"$inputDir/*")

  def run(jenkinsRdd: Option[RDD[String]], gerritRdd: Option[RDD[String]])
         (implicit sc: SparkSession): Dataset[Event] = {
    val jenkinsEvents = jenkinsRdd.map(_.jenkinsLogs.toEventFlow.filterChanges).toSeq
    val gerritEvents = gerritRdd.map(_.gerritEvents.toEventFlow.filterChanges).toSeq

    (jenkinsEvents ++ gerritEvents).reduce(_ union _).calculateDurations()
  }
}
