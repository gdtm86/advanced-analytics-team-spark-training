name := "intel-advanced-analytics"

version := "1.0"

scalaVersion := "2.10.6"


libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % "2.6.0" excludeAll ExclusionRule(organization = "org.eclipse.jetty"),
    "org.apache.spark" %% "spark-streaming" % "1.5.0",
    "org.apache.spark" %% "spark-core" % "1.5.0" excludeAll ExclusionRule(organization = "org.eclipse.jetty"),
    "org.apache.spark" %% "spark-streaming-twitter" % "1.5.0",
    "org.apache.spark" %% "spark-streaming-kafka" % "1.5.0",
    "org.apache.spark" %% "spark-sql" % "1.5.0",
    "joda-time" % "joda-time" % "2.9.2",
    "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Maven Central Server" at "http://repo1.maven.org/maven2"
)