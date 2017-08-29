package com.gerritforge.jw2017.model

import com.gerritforge.jw2017.model.gerrit.{CommentAddedEvent, PatchsetCreatedEvent, RefUpdatedEvent}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import CustomFormats._

class GerritEventSpec extends FlatSpec with Matchers with TestDataFixture {

  "Gerrit formatter" should "convert ref-updated event" in {
    parse(refUpdatedEventRawJson).extract[RefUpdatedEvent] should be(refUpdatedEvent)
  }

  it should "convert patchset-created event" in {
    parse(patchsetCreatedEventRawJson).extract[PatchsetCreatedEvent] should be(patchsetCreatedEvent)
  }

  it should "convert comment-added event" in {
    parse(commentAddedEventRawJson).extract[CommentAddedEvent] should be(commentAddedEvent)
  }
}
