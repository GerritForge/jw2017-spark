package com.gerritforge.jw2017.model

import java.sql.Timestamp
import java.util.UUID

case class Event(timestamp: Timestamp,
                 epoch: Long,
                 id: String,
                 changeNum: Option[Int],
                 patchSet: Option[Int],
                 duration: Option[Long],
                 rag: String,
                 source: String,
                 uri: String,
                 who: String,
                 what: String,
                 message: String,
                 project: String,
                 branch: String)
