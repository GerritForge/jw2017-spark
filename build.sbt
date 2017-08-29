name := "jenkins-gerrit-events-etl"

version := "1.0"

scalaVersion := "2.11.11"

resolvers += "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
    exclude("org.spark-project.spark", "unused"),
  "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"
    exclude("org.spark-project.spark", "unused"),
  "org.elasticsearch" %% "elasticsearch-spark-20" % "5.0.2"
    excludeAll ExclusionRule(organization = "org.apache.spark"),

  "org.json4s" %% "json4s-native" % "3.2.11",
  "com.github.scopt" %% "scopt" % "3.6.0",

  "org.scalactic" %% "scalactic" % "3.0.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

assemblyJarName in assembly := "jenkins-gerrit-events-etl-shaded.jar"

mainClass in (Compile,run) := Some("com.gerritforge.jw2017.job.Main")

parallelExecution in Test := false
