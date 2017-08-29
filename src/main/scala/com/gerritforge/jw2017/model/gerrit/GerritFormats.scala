package com.gerritforge.jw2017.model.gerrit

import java.sql.Timestamp

import org.json4s.JsonAST.JNull
import org.json4s.{CustomSerializer, JArray, JObject}
import com.gerritforge.jw2017.model.CustomConversions._


object GerritFormats {

  import com.gerritforge.jw2017.model.CustomFormats._

  class PatchsetCreatedEventSerializer extends CustomSerializer[PatchsetCreatedEvent](format => ( {
    case j: JObject =>
      val ts: Timestamp = j \ "eventCreatedOn"
      new PatchsetCreatedEvent(
        timestamp = ts,
        project = j \ "project" \ "name",
        revision = j \ "patchSet" \ "revision",
        refName = j \ "refName",
        committer = j \ "patchSet" \ "uploader",
        changeId = j \ "changeKey" \ "id",
        changeNumber = j \ "change" \ "number",
        patchsetNumber = j \ "patchSet" \ "number",
        owner = j \ "change" \ "owner",
        author = j \ "patchSet" \ "author",
        subject = j \ "change" \ "subject",
        commitMessage = j \ "change" \ "commitMessage",
        branch = j \ "change" \ "branch",
        url = j \ "change" \ "url",
        status = j \ "change" \ "status")
  }, {
    case _ => ???
  }))

  class RefUpdatedEventSerializer extends CustomSerializer[RefUpdatedEvent](format => ( {
    case j: JObject =>
      val ts: Timestamp = j \ "eventCreatedOn"
      new RefUpdatedEvent(timestamp = ts,
        project = j \ "refUpdate" \ "project",
        revision = j \ "refUpdate" \ "newRev",
        refName = j \ "refUpdate" \ "refName",
        committer = j \ "submitter",
        oldRevision = j \ "refUpdate" \ "oldRev")
  }, {
    case _ => ???
  }))

  class CommentAddedEventSerializer extends CustomSerializer[CommentAddedEvent](format => ( {
    case j: JObject =>
      val approval = (j \ "approvals").asInstanceOf[JArray].arr.find { j => j \ "oldValue" != JNull }

      new CommentAddedEvent(timestamp = j \ "eventCreatedOn",
        project = j \ "project" \ "name",
        revision = j \ "patchSet" \ "revision",
        refName = j \ "patchSet" \ "ref",
        committer = j \ "patchSet" \ "uploader",
        from = j \ "author",
        comment = j \ "comment",
        label = approval.map(_ \ "type"),
        value = approval.map(_ \ "value"),
        description = approval.map(_ \ "description"),
        changeNumber = j \ "change" \ "number",
        patchsetNumber = j \ "patchSet" \ "number"
      )
  }, {
    case _ => ???
  }))

  val formats: Seq[CustomSerializer[_]] = Seq(new PatchsetCreatedEventSerializer,
    new RefUpdatedEventSerializer,
    new CommentAddedEventSerializer)
}
