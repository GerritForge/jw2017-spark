package com.gerritforge.jw2017.etl

import java.net.URI

import com.gerritforge.jw2017.model.Event
import com.gerritforge.jw2017.model.gerrit.{CommentAddedEvent, GerritEvent, PatchsetCreatedEvent, RefUpdatedEvent}
import com.gerritforge.jw2017.model.jenkins.JobBuildLog
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write
import com.gerritforge.jw2017.model.CustomConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Transformers {

  implicit class PimpedJenkinsRdd(val rdd: RDD[JobBuildLog]) extends AnyVal {
    def changeNum(branch: String): Option[Int] = {
      val changePattern = "[0-9][0-9]/([0-9]+)/[0-9]+".r
      branch match {
        case changePattern(change) => Some(change.toInt)
        case _ => None
      }
    }

    def patchSet(branch: String): Option[Int] = {
      val patchsetPattern = "[0-9][0-9]/[0-9]+/([0-9]+)".r
      branch match {
        case patchsetPattern(patchset) => Some(patchset.toInt)
        case _ => None
      }
    }

    def toEventFlow: RDD[Event] = {

      rdd.map { l =>
        Event(l.timestamp,
          l.timestamp.getTime,
          uuidOf(l.timestamp, l.toString),
          changeNum(l.branch),
          patchSet(l.branch),
          None,
          "",
          "jenkins",
          l.url,
          "jenkins",
          "build",
          l.message,
          new URI(l.scmUri).getPath.drop(1),
          l.branch)
      }
    }
  }

  implicit class PimpedGerritRdd(val rdd: RDD[GerritEvent]) extends AnyVal {

    def toEventFlow: RDD[Event] = {
      rdd.collect {
          case e: PatchsetCreatedEvent =>
            Event(e.timestamp,
              e.timestamp.getTime,
              uuidOf(e.timestamp,e.toString),
              Some(e.changeNumber),
              Some(e.patchsetNumber),
              None,
              "green",
              "gerrit",
              e.url,
              e.committer.email,
              e.`type`,
              "",
              e.project,
              e.refName)

          case e: CommentAddedEvent =>
            Event(e.timestamp,
              e.timestamp.getTime,
              uuidOf(e.timestamp, e.toString),
              Some(e.changeNumber),
              Some(e.patchsetNumber),
              None,
              "green",
              "gerrit",
              "",
              e.from.email,
              e.`type`,
              e.comment,
              e.project,
              e.refName)

          case e: RefUpdatedEvent =>
            Event(e.timestamp,
              e.timestamp.getTime,
              uuidOf(e.timestamp, e.toString),
              None,
              None,
              None,
              "green",
              "gerrit",
              "",
              e.committer.email,
              e.`type`,
              "",
              e.project,
              e.refName)

          case e: GerritEvent =>
            Event(e.timestamp,
              e.timestamp.getTime,
              uuidOf(e.timestamp, e.toString),
              None,
              None,
              None,
              "green",
              "gerrit",
              "",
              e.committer.email,
              e.`type`,
              "",
              e.project,
              e.refName)
      }
    }
  }

  implicit class PimpedEventRdd(val rdd: RDD[Event]) {

    def toJson: RDD[String] = {
      rdd.map(write(_)(DefaultFormats))
    }

    def calculateDurations()(implicit sc: SparkSession): Dataset[Event] = {
      import sc.sqlContext.implicits._

      rdd.toDF
        .withColumn("duration",
          col("epoch") - lag(col("epoch"), 1)
            .over(Window
              .partitionBy("changeNum")
              .orderBy("epoch"))).as[Event]
    }

    def filterChanges: RDD[Event] = rdd.filter(_.changeNum.isDefined)
  }
}
