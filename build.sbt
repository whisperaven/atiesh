/*
 * Copyright (C) Hao Feng
 */

import sbt.Credentials
import sbt.Keys.{ credentials, publishTo }

lazy val common = Seq(
  organization := "io.0ops",
  /* master branch stable version => 2.2.1 (current)
   *   version := "2.2.1", */
  /* develop branch unstable test 2.3.x => stable release 2.4.x */
  version := "2.3.0",
  scalaVersion := "2.11.12",

  /* BuildPaths.defaultGlobalBase => ~/.sbt */
  credentials += Credentials(BuildPaths.defaultGlobalBase / ".credentials")
)

/*
 * default builds:
 *  => core
 *  => httputils => http
 *  => kafka
 */
lazy val distribution = Project(
  id = "atiesh-distribution",
  base = file(".")
).aggregate(core, httputils, kafka, http)
 .settings(common)

/* atiesh core project */
lazy val dependencies = Seq(
    // akka-actor
    "com.typesafe.akka"          %% "akka-actor"          % "2.5.26",
    // kamon
    "io.kamon"                   %% "kamon-core"          % "2.0.0"
      exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12")
      exclude("com.typesafe", "config"),
    "io.kamon"                   %% "kamon-system-metrics"% "2.0.0",
    /* kamon reporter dependency for test only */
    // "io.kamon"                   %% "kamon-prometheus"    % "2.0.0",
    // logger
    "ch.qos.logback"              % "logback-classic"     % "1.2.3", // scalalogging docs
    "com.typesafe.scala-logging" %% "scala-logging"       % "3.9.0"
)

/* atiesh core framework */
lazy val core = (project in file("core"))
  .settings(
    common,
    name := "atiesh",
    libraryDependencies ++= dependencies
  )

/* atiesh utils - http */
lazy val httputils = (project in file("utils/http"))
  .settings(
    common,
    name := "atiesh-utils-http",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.5.26"
        exclude("com.typesafe", "config"),
      "com.typesafe.akka" %% "akka-http" % "10.1.10",
    )
  ).dependsOn(core)

/* atiesh semantics - kafka */
lazy val kafka = (project in file("semantics-kafka"))
  .settings(
    common,
    name := "atiesh-semantics-kafka",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "1.1.1"
        exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12")
        exclude("com.typesafe.scala-logging", "scala-logging_2.11")
        exclude("com.typesafe.scala-logging", "scala-logging_2.12")
    )
  ).dependsOn(core)

/* atiesh semantics - http */
lazy val http = (project in file("semantics-http"))
  .settings(
    common,
    name := "atiesh-semantics-http",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.5.26"
        exclude("com.typesafe", "config"),
      "com.typesafe.akka" %% "akka-http" % "10.1.10",
    )
  ).dependsOn(httputils)

/*
 * atiesh semantics - syslog (experiment),
 *      we don't build this by default
 *
 * you can build it manual via sbt shell:
 *      sbt> project syslog
 *      sbt:atiesh-semantics-syslog> clean
 *      sbt:atiesh-semantics-syslog> package (or publish)
 */
lazy val syslog = (project in file("semantics-syslog"))
  .settings(
    common,
    name := "atiesh-semantics-syslog",
    libraryDependencies ++= Seq(
      "com.cloudbees" % "syslog-java-client" % "1.1.4"
    )
  ).dependsOn(core)

/*
 * atiesh semantics - aliyun,
 *      too much deps with conflicts warns,
 *      we don't build this by default
 *
 * you can build it manual via sbt shell:
 *      sbt> project aliyun
 *      sbt:atiesh-semantics-aliyun> clean
 *      sbt:atiesh-semantics-aliyun> package (or publish)
 */
lazy val aliyun = (project in file("semantics-aliyun"))
  .settings(
    common,
    name := "atiesh-semantics-aliyun",
    libraryDependencies ++= Seq(
      "com.aliyun.openservices" % "aliyun-log-producer" % "0.2.0"
        exclude("org.slf4j", "slf4j-api") exclude("org.slf4j", "slf4j-log4j12")
        exclude("ch.qos.logback", "logback-core") exclude("ch.qos.logback", "logback-classic"),
    )
  ).dependsOn(core)
