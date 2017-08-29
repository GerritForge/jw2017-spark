package com.gerritforge.jw2017.job


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.gerritforge.jw2017.etl.Transformers._
import org.elasticsearch.spark._
import org.json4s.DefaultFormats

case class MainOptions(gerritInputDir: Option[String] = None,
                       jenkinsInputDir: Option[String] = None,
                       outputDir: Option[String] = None,
                       elasticIndex: Option[String] = None)

object Main extends App with Job {

  new scopt.OptionParser[MainOptions]("scopt") {
    head("scopt", "3.x")

    opt[String]('g', "gerrit") optional() action { (x, c) =>
      c.copy(gerritInputDir = Some(x))
    } text "Gerrit input directory"

    opt[String]('j', "jenkins") optional() action { (x, c) =>
      c.copy(jenkinsInputDir = Some(x))
    } text "Jenkins input directory"

    opt[String]('o', "out") optional() action { (x, c) =>
      c.copy(outputDir = Some(x))
    } text "output directory"

    opt[String]('e', "elasticIndex") optional() action { (x, c) =>
      c.copy(elasticIndex = Some(x))
    } text "output directory"

  }.parse(args, MainOptions()) match {
    case Some(conf) =>
      val sparkConf = new SparkConf().setAppName("Gerrit Jenkins ETL")

      implicit val spark = SparkSession.builder()
        .appName("Gerrit Jenkins events ETL")
        .config(sparkConf)
        .getOrCreate()

      if ((conf.jenkinsInputDir ++ conf.gerritInputDir).isEmpty) {
        System.err.println(
          "Neither Jenkins nor Gerrit input directories have been defined\n" +
            "There are no events to be extracted.")
      } else {

        val outRdd = run(
          conf.jenkinsInputDir.map(loadTextFiles),
          conf.gerritInputDir.map(loadTextFiles))

        conf.outputDir foreach { outRdd.rdd.toJson.coalesce(1).saveAsTextFile (_) }
        conf.elasticIndex foreach { outRdd.rdd.saveToEs(_, Map("es.mapping.id" -> "id")) }
      }

    case None => // invalid configuration usage has been displayed
  }
}
