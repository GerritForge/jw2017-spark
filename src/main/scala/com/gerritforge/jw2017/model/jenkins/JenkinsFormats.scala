package com.gerritforge.jw2017.model.jenkins

import com.gerritforge.jw2017.model.CustomFormats._
import org.json4s._
import org.json4s.{CustomSerializer, JObject, JsonAST}
import com.gerritforge.jw2017.model.CustomConversions._

object JenkinsFormats {

  class JobBuildLogSerializer extends CustomSerializer[JobBuildLog](format => ( {
    case j: JObject =>
      val baseUrl: String = j \ "source_host"
      val path: String = j \ "data" \ "url"
      val environment: Map[String, String] = (j \ "data" \ "buildVariables").asInstanceOf[JObject]

      val urisString: List[String] = for(
        JArray(scmList: List[JValue]) <- j \ "data" \ "scms";
        JObject(scmFields) <- scmList;
        JField("repos", uriList) <- scmFields;
        JArray(urls) <- uriList;
        JString(urlsString) <- urls
      ) yield urlsString

      JobBuildLog(
        timestamp = j \ "epoch",
        url = baseUrl + path,
        buildNum = j \ "data" \ "buildNum",
        scmUri = urisString.headOption.fold("")(identity),
        duration = j \ "data" \ "buildDuration",
        host = j \ "data" \ "buildHost",
        environment = environment,
        branch = environment.get("BRANCH_NAME").fold("")(identity),
        message = (j \ "message").extract[List[String]].mkString("\n")
      )
  }, {
    case _ => ???
  }))

}
