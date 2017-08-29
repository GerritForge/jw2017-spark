package com.gerritforge.jw2017.common

import org.apache.spark.sql.SparkSession

trait SparkTestSupport {
  def localSparkSessionBuilder = SparkSession.builder().master("local").appName(getClass.getSimpleName)

  def withSpark(test: (SparkSession) => Any) = {
    val session = localSparkSessionBuilder.getOrCreate()
    try {
      test(session)
    } finally {
      session.stop()
    }
  }

}
