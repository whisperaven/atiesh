# Change Log
All changes from version 1.1.1 will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [2.4.3] - 2021-12-07
- add custom delimiter opt to http source impl
- update deps,
  - akka-http 10.1.13 -> 10.1.15 (latest of 10.1.x)
  - config 1.3.3 -> 1.4.1 (latest)
  - kamon 2.0.4 -> 2.0.5 (latest of 2.0.x)
  - scala-logging 3.9.0 -> 3.9.4 (latest)
  - slf4j-api 1.7.29 -> 1.7.32 (latest)
  - scala kafka to java kafka-clients 2.4.1
- change kafka `poll` invoke
- add `getJDuration` to *Configuration* class for read java duration object

## [2.4.2] - 2021-04-25
- fix KafkaLimitAckSinkSemantics `promise already completed` error
- add new `openFile` and `getLinesIterator` api to filesystem source semantics
- add new `IO` utils for simple file IO operations

## [2.4.1] - 2021-03-08
- add new config for directory watch source for handle large line
- fix http pool bufferoverflow
- fix http pool shutdown block forever
- update akka deps

## [2.4.0] - 2020-12-25 (Merry Christmas)
- add new semantics for http source
- add new simple components for http source/sink
- add new semantics for filesystem source
- add new simple component for filesystem source
- add new semantics for kafka sink
- add new simple component for kafka sink
- redesign components and semantics api
- redesign components and server initialize
- redesign utils for http request/response
- update Kamon/Akka-Http modules version
- bug fix for concurrent/startup/shutdown issues

## [2.1.0] - 2019-04-10
- add commit to trait Sink
- add start & stop to trait Sink\* and trait Source\*
- add try/catch inside Sink/Source actor for Open/Close
- add new trait KafkaSinkSemantics for kafka producer sink
- change KafkaSource, rename setting from client-settings to kafka-properties
- change KafkaSource, move consumer creation from bootstarp to start
- change some var/val name in Sink\*Semantics

## [2.0.1] - 2019-03-23
- new style of kamon metrics initialize, reporter initialized via classloader
- add kamon metrics create with measurement unit
- add several kamon counters for core sink
- remove some import statements
- remove kamon-prometheus from core dependencies
- rename root project name for avoid conflict
