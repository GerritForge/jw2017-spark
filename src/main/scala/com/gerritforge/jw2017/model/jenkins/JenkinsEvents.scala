package com.gerritforge.jw2017.model.jenkins

import java.sql.Timestamp

import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory
import com.gerritforge.jw2017.model.CustomFormats._


trait JenkinsEvent {
  def timestamp: Timestamp

  def url: String

  def buildNum: Int

  def scmUri: String
}

case class JobBuildLog(timestamp: Timestamp,
                       url: String,
                       buildNum: Int,
                       scmUri: String,
                       duration: Long,
                       host: String,
                       environment: Map[String, String],
                       message: String,
                       branch: String) extends JenkinsEvent

object JenkinsEvent {
  private val log = LoggerFactory.getLogger(classOf[JenkinsEvent])

  def apply(eventRawJson: String): Option[JobBuildLog] = try {
    Some(parse(eventRawJson).extract[JobBuildLog])
  } catch {
    case e: Exception =>
      log.error(s"Unable to convert event '$eventRawJson'", e)
      None
  }
}
