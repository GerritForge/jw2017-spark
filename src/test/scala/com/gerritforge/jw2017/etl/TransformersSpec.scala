package com.gerritforge.jw2017.etl

import java.sql.Timestamp
import java.util.UUID

import com.gerritforge.jw2017.etl.Transformers._
import com.gerritforge.jw2017.model.Event
import com.gerritforge.jw2017.model.gerrit.GerritEvent
import org.scalatest.{Inside, Inspectors}
import org.json4s._
import org.json4s.native.JsonParser._

class TransformersSpec extends SparkEtlSpec with Inside with Inspectors {

  "Jenkins build log message" should
    "be transformed into an Event flow" in withSpark { sc =>

    val jobLogRdd = sc.sparkContext.parallelize(Seq(jobBuildLog))

    val events = jobLogRdd.toEventFlow.collect()
    events should have size 1
    inside(events.head) {
      case Event(ts, _, id, flowId, _, _, _, source, _, who, what, message, project, branch) =>

        ts should be(jobBuildLog.timestamp)
        id should not be null
        flowId should not be empty
        source should be("jenkins")
        who should be("jenkins")
        what should be("build")
        message should be(jobBuildLog.message)
        project should be("gerritforge/play-scala-chatroom-example")
        branch should be(jobBuildLog.environment("BRANCH_NAME"))
    }
  }

  "Gerrit ref-update stream event" should
    "be transformed into an Event flow" in withSpark { sc =>
    val gerritEventRdd = sc.sparkContext.parallelize(Seq(refUpdatedEvent.asInstanceOf[GerritEvent]))

    val events = gerritEventRdd.toEventFlow.collect()
    events should have size 1
    inside(events.head) {
      case Event(ts, _, id, _, _, _, rag, source, _, who, what, _, project, branch) =>

        ts should be(refUpdatedEvent.timestamp)
        id should not be null
        rag should be("green")
        source should be("gerrit")
        who should be(refUpdatedEvent.committer.email)
        what should be("ref-updated")
        project should be(refUpdatedEvent.project)
        branch should be(refUpdatedEvent.refName)
    }
  }

  "Gerrit patchset-created event" should
    "be transformed into an Event flow" in withSpark { sc =>
    val gerritEventRdd = sc.sparkContext.parallelize(Seq(patchsetCreatedEvent.asInstanceOf[GerritEvent]))

    val events = gerritEventRdd.toEventFlow.collect()
    events should have size 1
    inside(events.head) {
      case Event(ts, _, id, _, _, _, rag, source, _, who, what, _, project, branch) =>

        ts should be(patchsetCreatedEvent.timestamp)
        id should not be null
        rag should be("green")
        source should be("gerrit")
        who should be(patchsetCreatedEvent.committer.email)
        what should be("patchset-created")
        project should be(patchsetCreatedEvent.project)
        branch should be(patchsetCreatedEvent.refName)
    }
  }
}
