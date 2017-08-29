package com.gerritforge.jw2017.job

import com.gerritforge.jw2017.etl.SparkEtlSpec

class JobSpec extends SparkEtlSpec with Job {

  "Job" should "only transform Gerrit and Jenkins logs related to changes" in withSpark { implicit sc =>

    val gerritLogs = sc.sparkContext.parallelize(Seq(
      refUpdatedEventRawJson,
      patchsetCreatedEventRawJson,
      commentAddedEventRawJson))

    val jenkinsLogs = sc.sparkContext.parallelize(Seq(jobBuildLogRawJson))

    val events = run(Some(jenkinsLogs), Some(gerritLogs)).collect

    events should have size 3
    events.map(_.what) should not contain ("ref-updated")
  }

}
