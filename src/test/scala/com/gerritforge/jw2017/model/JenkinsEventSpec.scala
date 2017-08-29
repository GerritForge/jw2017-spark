package com.gerritforge.jw2017.model

import com.gerritforge.jw2017.model.jenkins.JobBuildLog
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import CustomFormats._

class JenkinsEventSpec extends FlatSpec with Matchers with TestDataFixture {

  "Jenkins Formatter" should
  "convert a jobLog" in {
    parse(jobBuildLogRawJson).extract[JobBuildLog] should be (jobBuildLog)
  }

}
