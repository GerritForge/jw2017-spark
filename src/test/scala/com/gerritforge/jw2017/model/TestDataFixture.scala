package com.gerritforge.jw2017.model

import java.sql.Timestamp
import java.util.UUID

import com.gerritforge.jw2017.model.gerrit.{CommentAddedEvent, PatchsetCreatedEvent, PersonIdent, RefUpdatedEvent}
import com.gerritforge.jw2017.model.jenkins.JobBuildLog
import CustomFormats._

trait TestDataFixture {

  val adminIdent = PersonIdent("Administrator", "admin@example.com", "admin")

  val jenkinsIdent = PersonIdent("Jenkins CI", "jenkins@localdomain", "jenkins")

  val refUpdatedEventRawJson =
    """
      |{
      |	"submitter": {
      |		"name": "Administrator",
      |		"email": "admin@example.com",
      |		"username": "admin"
      |	},
      |	"@timestamp": "2017-08-21T10:24:24.447Z",
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
    """.stripMargin

  val refUpdatedEvent = RefUpdatedEvent(
    timestamp = new Timestamp(1503311064000L),
    project = "gerritforge/play-scala-chatroom-example",
    revision = "ecc390299f4db260757e12c66ce2651eae78d7f6",
    refName = "refs/changes/33/33/4",
    committer = adminIdent,
    oldRevision = "0000000000000000000000000000000000000000")

  val patchsetCreatedEventRawJson =
    """
      |{
      |	"changeKey": {
      |		"id": "Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4"
      |	},
      |	"@timestamp": "2017-08-21T10:24:24.471Z",
      |	"uploader": {
      |		"name": "Administrator",
      |		"email": "admin@example.com",
      |		"username": "admin"
      |	},
      |	"change": {
      |		"owner": {
      |			"name": "Administrator",
      |			"email": "admin@example.com",
      |			"username": "admin"
      |		},
      |		"number": 33,
      |		"commitMessage": "Publish-Local\n\nChange-Id: Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4\n",
      |		"subject": "Publish-Local",
      |		"project": "gerritforge/play-scala-chatroom-example",
      |		"id": "Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4",
      |		"branch": "master",
      |		"url": "http://review-pipeline/33",
      |		"status": "NEW"
      |	},
      |	"@version": "1",
      |	"project": {
      |		"name": "gerritforge/play-scala-chatroom-example"
      |	},
      |	"eventCreatedOn": 1503311064,
      |	"refName": "refs/heads/master",
      |	"type": "patchset-created",
      |	"patchSet": {
      |		"number": 4,
      |		"sizeInsertions": 3,
      |		"ref": "refs/changes/33/33/4",
      |		"uploader": {
      |			"name": "Administrator",
      |			"email": "admin@example.com",
      |			"username": "admin"
      |		},
      |		"author": {
      |			"name": "Administrator",
      |			"email": "admin@example.com",
      |			"username": "admin"
      |		},
      |		"kind": "NO_CODE_CHANGE",
      |		"sizeDeletions": 0,
      |		"isDraft": false,
      |		"createdOn": 1503311064,
      |		"revision": "ecc390299f4db260757e12c66ce2651eae78d7f6",
      |		"parents": ["9b74ad231ae1ca1d214c327495a6aba8eaa6c38d"]
      |	}
      |}
    """.stripMargin

  val patchsetCreatedEvent = PatchsetCreatedEvent(
    timestamp = new Timestamp(1503311064000L),
    project = "gerritforge/play-scala-chatroom-example",
    revision = "ecc390299f4db260757e12c66ce2651eae78d7f6",
    refName = "refs/heads/master",
    committer = adminIdent,
    changeId = "Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4",
    changeNumber = 33,
    patchsetNumber = 4,
    owner = adminIdent,
    author = adminIdent,
    subject = "Publish-Local",
    commitMessage = "Publish-Local\n\nChange-Id: Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4\n",
    branch = "master",
    url = "http://review-pipeline/33",
    status = "NEW")

  val commentAddedEventRawJson =
    """
      |{
      |	"changeKey": {
      |		"id": "Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4"
      |	},
      |	"@timestamp": "2017-08-21T10:25:52.800Z",
      |	"author": {
      |		"name": "Jenkins CI",
      |		"email": "jenkins@localdomain",
      |		"username": "jenkins"
      |	},
      |	"approvals": [{
      |		"description": "Verified",
      |		"oldValue": "0",
      |		"type": "Verified",
      |		"value": "1"
      |	}, {
      |		"description": "Code-Review",
      |		"type": "Code-Review",
      |		"value": "0"
      |	}],
      |	"change": {
      |		"owner": {
      |			"name": "Administrator",
      |			"email": "admin@example.com",
      |			"username": "admin"
      |		},
      |		"number": 33,
      |		"commitMessage": "Publish-Local\n\nChange-Id: Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4\n",
      |		"subject": "Publish-Local",
      |		"project": "gerritforge/play-scala-chatroom-example",
      |		"id": "Ibf75aaf929fcf40afae5a43c1775bd1ee8a9a8d4",
      |		"branch": "master",
      |		"url": "http://review-pipeline/33",
      |		"status": "NEW"
      |	},
      |	"@version": "1",
      |	"project": {
      |		"name": "gerritforge/play-scala-chatroom-example"
      |	},
      |	"eventCreatedOn": 1503311152,
      |	"comment": "Patch Set 4: Verified+1\n\nIt works !",
      |	"refName": "refs/heads/master",
      |	"type": "comment-added",
      |	"patchSet": {
      |		"number": 4,
      |		"sizeInsertions": 3,
      |		"ref": "refs/changes/33/33/4",
      |		"uploader": {
      |			"name": "Administrator",
      |			"email": "admin@example.com",
      |			"username": "admin"
      |		},
      |		"author": {
      |			"name": "Administrator",
      |			"email": "admin@example.com",
      |			"username": "admin"
      |		},
      |		"kind": "NO_CODE_CHANGE",
      |		"sizeDeletions": 0,
      |		"isDraft": false,
      |		"createdOn": 1503311064,
      |		"revision": "ecc390299f4db260757e12c66ce2651eae78d7f6",
      |		"parents": ["9b74ad231ae1ca1d214c327495a6aba8eaa6c38d"]
      |	}
      |}
    """.stripMargin

  val commentAddedEvent = CommentAddedEvent(
    timestamp = new Timestamp(1503311152000L),
    project = "gerritforge/play-scala-chatroom-example",
    revision = "ecc390299f4db260757e12c66ce2651eae78d7f6",
    refName = "refs/changes/33/33/4",
    committer = adminIdent,
    from = jenkinsIdent,
    comment = "Patch Set 4: Verified+1\n\nIt works !",
    label = Some("Verified"),
    value = Some(1),
    description = Some("Verified"),
    changeNumber = 33,
    patchsetNumber = 4)

  val jobBuildLogRawJson =
    """
      |{
      |	"source_host": "http://jenkins-pipeline/",
      |	"epoch": 1503311100000,
      |	"data": {
      |   "scms": [
      |     {
      |       "repos": [
      |         "http://gerrit:8080/gerritforge/play-scala-chatroom-example"
      |       ],
      |       "branches": [
      |         "33/33/4"
      |       ]
      |     }
      |   ],
      |		"buildVariables": {
      |			"BUILD_TAG": "jenkins-pipeline-33%2F33%2F4-1",
      |			"RUN_DISPLAY_URL": "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/1/display/redirect",
      |			"HUDSON_HOME": "/var/jenkins_home",
      |			"RUN_CHANGES_DISPLAY_URL": "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/1/display/redirect?page=changes",
      |			"JOB_URL": "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/",
      |			"HUDSON_SERVER_COOKIE": "d4835bf5e81a8af3",
      |			"JOB_BASE_NAME": "33%2F33%2F4",
      |			"BUILD_ID": "1",
      |			"CLASSPATH": "",
      |			"JENKINS_HOME": "/var/jenkins_home",
      |			"JOB_NAME": "pipeline/33%2F33%2F4",
      |			"BUILD_NUMBER": "1",
      |			"BUILD_DISPLAY_NAME": "#1",
      |			"BUILD_URL": "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/1/",
      |			"HUDSON_URL": "http://jenkins-pipeline/",
      |			"JOB_DISPLAY_URL": "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/display/redirect",
      |			"JENKINS_SERVER_COOKIE": "d4835bf5e81a8af3",
      |			"BRANCH_NAME": "33/33/4",
      |			"JENKINS_URL": "http://jenkins-pipeline/"
      |		},
      |		"buildNum": 1,
      |		"rootProjectName": "33%2F33%2F4",
      |		"rootProjectDisplayName": "#1",
      |		"displayName": "#1",
      |		"fullDisplayName": "pipeline » 33/33/4 #1",
      |		"rootBuildNum": 1,
      |		"id": "1",
      |		"projectName": "33%2F33%2F4",
      |		"url": "job/pipeline/job/33%252F33%252F4/1/",
      |		"buildDuration": 52418,
      |		"buildHost": "Executor #-1"
      |	},
      |	"@version": 1,
      |	"source": "jenkins",
      |	"message": ["Building", " > git rev-parse --is-inside-work-tree # timeout=10"]
      |}
    """.stripMargin

  val jobBuildLog = JobBuildLog(
    timestamp = new Timestamp(1503311100000L),
    buildNum = 1,
    scmUri = "http://gerrit:8080/gerritforge/play-scala-chatroom-example",
    url = "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/1/",
    duration = 52418,
    host = "Executor #-1",
    environment = Map("BUILD_TAG" -> "jenkins-pipeline-33%2F33%2F4-1",
      "RUN_DISPLAY_URL" -> "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/1/display/redirect",
      "HUDSON_HOME" -> "/var/jenkins_home",
      "RUN_CHANGES_DISPLAY_URL" -> "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/1/display/redirect?page=changes",
      "JOB_URL" -> "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/",
      "HUDSON_SERVER_COOKIE" -> "d4835bf5e81a8af3",
      "JOB_BASE_NAME" -> "33%2F33%2F4",
      "BUILD_ID" -> "1",
      "CLASSPATH" -> "",
      "JENKINS_HOME" -> "/var/jenkins_home",
      "JOB_NAME" -> "pipeline/33%2F33%2F4",
      "BUILD_NUMBER" -> "1",
      "BUILD_DISPLAY_NAME" -> "#1",
      "BUILD_URL" -> "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/1/",
      "HUDSON_URL" -> "http://jenkins-pipeline/",
      "JOB_DISPLAY_URL" -> "http://jenkins-pipeline/job/pipeline/job/33%252F33%252F4/display/redirect",
      "JENKINS_SERVER_COOKIE" -> "d4835bf5e81a8af3",
      "BRANCH_NAME" -> "33/33/4",
      "JENKINS_URL" -> "http://jenkins-pipeline/"),
    branch = "33/33/4",
    message = "Building\n > git rev-parse --is-inside-work-tree # timeout=10")

  val gerritEvent = Event(new Timestamp(System.currentTimeMillis()),
    System.currentTimeMillis(),
    UUID.randomUUID().toString,
    Some(1),
    Some(2),
    None,
    "green",
    "gerrit",
    "http://somewhere.com/1/2",
    "submitter@company.com",
    "patchset-create",
    "",
    "myproject",
    "mybranch")

  val jenkinsEvent = Event(gerritEvent.timestamp,
    gerritEvent.epoch,
    UUID.randomUUID().toString,
    Some(1),
    Some(2),
    None,
    "green",
    "jenkins",
    "jenkins",
    "http://somewhere.com/1/2/job",
    "build",
    "Building file XYZ",
    "??",
    "mybranch")
}
