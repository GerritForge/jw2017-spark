package com.gerritforge.jw2017.etl

import java.time.LocalDateTime

import com.gerritforge.jw2017.model.gerrit.GerritEvent
import com.gerritforge.jw2017.model.jenkins.{JenkinsEvent, JobBuildLog}
import org.apache.spark.rdd.RDD


object Readers {

  implicit def dateTimeOrdering: Ordering[LocalDateTime] = Ordering.fromLessThan(_ isBefore _)

  implicit class PimpedRddStringEvents(val eventsRdd: RDD[String]) extends AnyVal {
    def gerritEvents: RDD[GerritEvent] = eventsRdd.flatMap(GerritEvent.apply)

    def jenkinsLogs: RDD[JobBuildLog] = eventsRdd.flatMap(JenkinsEvent.apply)
  }
}
