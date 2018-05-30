import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val kafkaStreams = "org.apache.kafka" % "kafka-streams" % "1.1.0"
  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.1.3"
}
