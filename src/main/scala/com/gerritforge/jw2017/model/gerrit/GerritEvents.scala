package com.gerritforge.jw2017.model.gerrit

import java.sql.Timestamp

import org.json4s._
import org.json4s.native.JsonMethods._
import com.gerritforge.jw2017.model.CustomFormats._

trait GerritEvent {
  def `type`: String

  def timestamp: Timestamp

  def project: String

  def revision: String

  def refName: String

  def committer: PersonIdent
}

case class RefUpdatedEvent(`type`: String = GerritEvent.RefUpdated,
                           timestamp: java.sql.Timestamp,
                           project: String,
                           revision: String,
                           refName: String,
                           committer: PersonIdent,

                           oldRevision: String) extends GerritEvent

case class PatchsetCreatedEvent(`type`: String = GerritEvent.PatchSetCreated,
                                timestamp: java.sql.Timestamp,
                                project: String,
                                revision: String,
                                refName: String,
                                committer: PersonIdent,

                                changeId: String,
                                changeNumber: Int,
                                patchsetNumber: Int,
                                owner: PersonIdent,
                                author: PersonIdent,
                                subject: String,
                                commitMessage: String,
                                branch: String,
                                url: String,
                                status: String) extends GerritEvent

case class CommentAddedEvent(`type`: String = GerritEvent.CommentAdded,
                             timestamp: java.sql.Timestamp,
                             project: String,
                             revision: String,
                             refName: String,
                             committer: PersonIdent,
                             from: PersonIdent,
                             comment: String,
                             label: Option[String],
                             value: Option[Int],
                             description: Option[String],
                             changeNumber: Int,
                             patchsetNumber: Int) extends GerritEvent

case class PersonIdent(name: String, email: String, username: String)

object GerritEvent {

  val RefUpdated = "ref-updated"
  val PatchSetCreated = "patchset-created"
  val CommentAdded = "comment-added"

  def apply(raw: String): Option[GerritEvent] = {
    val json = parse(raw)
    (json \ "type") match {
      case JString(PatchSetCreated) => Some(json.extract[PatchsetCreatedEvent])
      case JString(RefUpdated) => Some(json.extract[RefUpdatedEvent])
      case JString(CommentAdded) => Some(json.extract[CommentAddedEvent])
      case _ => None
    }
  }
}
