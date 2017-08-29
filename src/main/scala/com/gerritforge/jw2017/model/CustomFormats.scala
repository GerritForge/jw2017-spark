package com.gerritforge.jw2017.model

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.util.UUID

import com.gerritforge.jw2017.model.gerrit.GerritFormats.{CommentAddedEventSerializer, PatchsetCreatedEventSerializer, RefUpdatedEventSerializer}
import com.gerritforge.jw2017.model.gerrit.PersonIdent
import com.gerritforge.jw2017.model.jenkins.JenkinsFormats.JobBuildLogSerializer
import org.json4s.native.Serialization
import org.json4s.{CustomSerializer, JValue, NoTypeHints, _}


object CustomFormats {
  implicit val formats = Serialization.formats(NoTypeHints) +
    new PatchsetCreatedEventSerializer +
    new RefUpdatedEventSerializer +
    new CommentAddedEventSerializer +
    new JobBuildLogSerializer

}

object CustomConversions {
  private lazy val dateParser = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")

  trait Seed {
    def getSeed: Array[Byte]
  }

  implicit def jvalueToString(jvalue: JValue): String = {
    implicit val formats = DefaultFormats
    jvalue.extract[String]
  }

  implicit def jvalueToPersonIdent(jvalue: JValue): PersonIdent = jvalue match {
    case JObject(personFields) =>
      val personIdent = (for (
        JField("name", JString(name)) <- personFields;
        JField("email", JString(email)) <- personFields;
        JField("username", JString(username)) <- personFields)
        yield PersonIdent(name, email, username)).headOption

      personIdent.fold(PersonIdent("", "", ""))(identity)

    case _ => PersonIdent("", "", "")
  }

  implicit def stringToTimestamp(timestampString: String): Timestamp =
    new Timestamp(LocalDateTime.parse(timestampString, dateParser)
      .atOffset(ZoneOffset.UTC)
      .toInstant
      .toEpochMilli)

  implicit def timestampToString(timestamp: Timestamp): String =
    ZonedDateTime.ofInstant(
      LocalDateTime.ofEpochSecond(timestamp.getTime / 1000L, ((timestamp.getTime % 1000) * 1000000).toInt, ZoneOffset.UTC),
      ZoneOffset.UTC, ZoneId.of("Z")
    ) format dateParser

  implicit def jvalueToTimestamp(jvalue: JValue): Timestamp = jvalue match {
    case JString(s) => stringToTimestamp(s)
    case JInt(epoch) => new Timestamp(if (epoch.toLong < 100000000000L) epoch.toLong * 1000L else epoch.toLong)
    case _ => throw new IllegalArgumentException(s"Cannot convert $jvalue into a Timestamp")
  }

  implicit def jvalueToLong(jvalue: JValue): Long = jvalue match {
    case JInt(bigInt) => bigInt.toLong
    case JString(stringValue) => stringValue.toLong
    case _ => throw new IllegalArgumentException(s"Cannot convert $jvalue to a Long")
  }

  implicit def jvalueToInt(jvalue: JValue): Int = jvalue match {
    case JInt(bigInt) => bigInt.toInt
    case JString(stringValue) => stringValue.toInt
    case _ => throw new IllegalArgumentException(s"Cannot convert $jvalue to a Int")
  }

  implicit def jobjectToMap(jvalue: JObject): Map[String, String] = {
    implicit val formats = DefaultFormats
    jvalue match {
      case JObject(fields: Seq[JField]) => fields.map(jfield => (jfield._1, jfield._2.extract[String])).toMap
      case _ => throw new IllegalArgumentException(s"Cannot convert $jvalue to a Map[String,String]")
    }
  }

  implicit def jarrayToSeq(jvalue: JArray): Seq[String] = {
    implicit val formats = DefaultFormats
    jvalue match {
      case JArray(jvalues) => jvalues.map(_.extract[String])
      case _ => throw new IllegalArgumentException(s"Cannot convert $jvalue to a Seq[String]")
    }
  }

  def uuidOf(ts: Timestamp, seed: String): String = {
    val buffer = ByteBuffer.allocate(8 + seed.length)
    buffer.put(seed.getBytes)
    buffer.putLong(ts.getTime)
    UUID.nameUUIDFromBytes(buffer.array).toString
  }
}


