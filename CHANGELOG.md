# Change Log
All changes from version 1.1.1 will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

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
