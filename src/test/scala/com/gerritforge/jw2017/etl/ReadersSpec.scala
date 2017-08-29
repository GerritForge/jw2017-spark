package com.gerritforge.jw2017.etl

import com.gerritforge.jw2017.common.SparkTestSupport
import com.gerritforge.jw2017.etl.Readers._
import com.gerritforge.jw2017.model.TestDataFixture
import com.gerritforge.jw2017.model.gerrit.{PersonIdent, RefUpdatedEvent}
import org.scalatest.{FlatSpec, Inside, Matchers}

class ReadersSpec extends SparkEtlSpec with Inside {

  "Readers" should "decode an RDD of Gerrit events" in withSpark { sc =>
    val gerritEventsRdd = sc.sparkContext.parallelize(
      Seq(
        refUpdatedEventRawJson,
        patchsetCreatedEventRawJson,
        commentAddedEventRawJson))
    val gerritEvents = gerritEventsRdd.gerritEvents.collect

    gerritEvents should contain allOf(
      refUpdatedEvent,
      patchsetCreatedEvent,
      commentAddedEvent)
  }

  it should "allow a ref-updated event without submitter" in withSpark { sc =>
    val gerritEventRdd = sc.sparkContext.parallelize(Seq(
      """
        |{
        | "@timestamp": "2017-08-21T10:24:24.447Z",
        |	"@version": "1",
        |	"eventCreatedOn": 1503311064,
        |	"refUpdate": {
        |		"oldRev": "0000000000000000000000000000000000000000",
        |		"newRev": "ecc390299f4db260757e12c66ce2651eae78d7f6",
        |		"project": "gerritforge/play-scala-chatroom-example",
        |		"refName": "refs/changes/33/33/4"
        |	},
        |	"type": "ref-updated"
        |}
      """.stripMargin))

    val gerritEvent = gerritEventRdd.gerritEvents.collect

    gerritEvent should have size 1
    inside(gerritEvent.head) {
      case RefUpdatedEvent(_, _, _, _, _, committer, _) =>
        committer should be (PersonIdent("","",""))
    }
  }

  it should "decode an RDD of Jenkins events" in withSpark { sc =>
    val jenkinsEventsRdd = sc.sparkContext.parallelize(
      Seq(jobBuildLogRawJson)
    )
    val jenkinsLogs = jenkinsEventsRdd.jenkinsLogs.collect

    jenkinsLogs should contain (jobBuildLog)
  }
}
