# Atiesh

**Atiesh** is an open-source stream-processing framework, developed on top of **akka-actor** with builtin simple and reliable backpressure mechanism.

In WoW, Atiesh is the *Greatstaff of the Guardian*, hope that you use it open a portal for your data-stream just like what Medivh do for the Orc :>

# Components and Events

There are three types of basic components for construct your stream data flow

- Source
- Interceptor
- Sink

And messages which passed through them are wrapped into *event*, the default event class is *atiesh.event.SimpleEvent*

The *Extension* type of component is use as helper & support component (e.g.:*atiesh.utils.http.CachedProxy*), which based on *atiesh.utils.AtieshExtension*

## Life cycle and basic unit of data stream

The basic unit of data (or messages) is represented as *event*, they were consumed or accepted by *source* from some external system and wrapped into *event* class, intercepted (modify or filter) by one or more specified *Interceptor*, and finally send to one *sink* which drain these *event* (e.g.: wrap it/them into http request, batch process, etc.) and send it to some external system, right after that, the messages processing for a batch or a single message is completed

## Built-in Components List

| Component Name                 | Type          | Usage                                    |
| :----------------------------: | :-----------: | :--------------------------------------: |
| atiesh.source.DevZero          | Source        | generate and produce string "0"          |
| atiesh.source.KafkaSource      | Source        | poll event from kafka brokers            |
| atiesh.source.HttpSource       | Source        | extra events from incoming http requests |
| atiesh.interceptor.DevNull     | Interceptor   | intercept and discard everything         |
| atiesh.interceptor.Transparent | Interceptor   | intercept and forward everything         |
| atiesh.sink.DevNull            | Sink          | drain and discard everything             |
| atiesh.sink.KafkaSink          | Sink          | drain and write event to kafka           |
| atiesh.sink.HttpSink           | Sink          | drain and write event to http remote     |
| atiesh.utils.http.CachedProxy  | Extension     | external http access & cache tool        |

- These sink and source components provide very simple functionality just for demonstrate component development
- The *DevNull* *sink* drain and discard everything, which means you can use it to avoid noise like *no sink found* inside your log
- The *CachedProxy* *extension* can be use as simple remote configuration syncer

# Components and Semantics

*Component* is the basic abstract to implement you own data processor (act as *sink* or *source* or *interceptor*). On the other hand, *Semantics* is the abstract layer to implement your own protocols (e.g.: talk to http, communicate with kafka), they were intended to be used as mixins of your *Component* implemenation (the Scala *cake pattern*) via ``extends ... with ... with ...`` inside you class definition

The built-in components and semantics was splited to several different projects, and thanks to that, we now have a very tiny core project with a few dependencies

| GroupID                    | ArtifactID           | Description                |
| :------------------------: | :------------------: | :------------------------: |
| com.typesafe               | config               | configuration read & parse |
| io.kamon                   | kamon-core           | monitor data collection    |
|                            | kamon-system-metrics | monitor data support       |
| com.typesafe.akka          | akka-actor           | core actor library         |
| com.typesafe.scala-logging | scala-logging        | logging support            |

## Built-in Semantics

The *BatchSinkSemantics* was inside the core project, others are not

| Semantics Class FQDN                        | Semantics Type  | Descriptions                           |
| :-----------------------------------------: | :-------------: | :------------------------------------: |
| atiesh.sink.BatchSinkSemantics              | SinkSemantics   | Batch Mode Support                     |
| atiesh.sink.HttpSinkSemantics               | SinkSemantics   | Protocol Support - Http Client         |
| atiesh.sink.HttpSourceSemantics             | SourceSemantics | Protocol Support - Http Server         |
| atiesh.sink.KafkaSinkSemantics              | SinkSemantics   | Protocol Support - Kafka Producer      |
| atiesh.source.KafkaSourceSemantics          | SourceSemantics | Protocol Support - Kafka Consumer      |
| atiesh.source.DirectoryWatchSourceSemantics | SourceSemantics | Protocol Support - FS Directory Watch  |
| atiesh.sink.SyslogSinkSemantics             | SinkSemantics   | Protocol Support - Syslog              |
| atiesh.sink.AliyunSLSSinkSemantics          | SinkSemantics   | Protocol Support - Aliyun SLS Producer |

## API Levels

Only the *source* and *sink* components can extends by semantics, and their api was splited to three levels

- *Component API*, component abstract api, should implement the event process method and component startup and shutdown method
- *Semantics API*, semantics abstract api, provide a way to interact with the *ActorSystem* of *akka-actor* and the *backpressure/start/stop* transactions (internally, they are Scala `Promise`)
- *Internal API*, internal abstract api, just for declare the basic abstract method and implement the internal *Actor* of *akka-actor* of that component, and you should not touch any of these

# License

Atiesh is Open Soure and available under the MIT License

# Author and Maintainer

Hao Feng < whisperaven@gmail.com >
