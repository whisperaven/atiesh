# Atiesh

**埃提耶什 (Atiesh)** 是一个开源流处理开发框架, 基于 **akka-actor (classic)** 开发并内置简单可靠的背压 (backpressure) 机制

在魔兽世界中, 埃提耶什是艾泽拉斯守护者的法杖, 希望你能用它为你的数据流打开一个传送门, 就像麦迪文为兽人做的那样 :>

# 组件和事件 (Components & Events)

用于构建数据流的基础组件, 一共包含三种, 它们包括

- 来源 (Source)
- 拦截器 (Interceptor)
- 去向 (Sink)

而在它们之中传递的消息则被包装成事件 (Event) 当前默认等事件类为 *atiesh.event.SimpleEvent*

类型为 *Extension* 的组件为支持组件 (e.g.: *atiesh.utils.http.CachedProxy*) 基于 *atiesh.utils.AtieshExtension* 开发

## 数据流的生命周期和单位

数据 (或者说消息) 的最小单位是 *event* , 它们被 *source* 从外部服务接收或读取并包装成 *event*, 经由给定的 *interceptor* 去做处理(数据清洗或过滤, 可按需丢弃), 最后将处理后的 *event* 发送给特定的 *sink* 并由这个 *sink* 将其加工发送给外部系统 (e.g.: 包装 Http Request, 批处理等) , 至此一条或者一批的消息算处理完毕

## 内置组件列表

|              组件名称              |    类型     |              用途               |
| :--------------------------------: | :---------: | :-----------------------------: |
|       atiesh.source.DevZero        |   Source    |    生成消息内容为 "0" 的消息    |
| atiesh.source.DirectoryWatchSource |   Source    |  读取本地目录中的文件获取消息   |
|     atiesh.source.KafkaSource      |   Source    |       消费 Kafka 消息队列       |
|      atiesh.source.HttpSource      |   Source    |  接收 Http 请求并从中获取消息   |
|     atiesh.interceptor.DevNull     | Interceptor |       拦截并丢弃所有消息        |
|   atiesh.interceptor.Transparent   | Interceptor |       拦截并放行所有消息        |
|        atiesh.sink.DevNull         |    Sink     |       接收并丢弃所有消息        |
|       atiesh.sink.KafkaSink        |    Sink     | 接收并将消息写入 Kafka 消息队列 |
|        atiesh.sink.HttpSink        |    Sink     | 接收并将消息写入 Http 远端服务  |
|   atiesh.utils.http.CachedProxy    |  Extension  |    外部 Http 访问 & 缓存工具    |

- 这些 *Source* 和 *Sink* 组件只提供非常简单的功能, 意在当作简单的组件开发示例
- *DevNull* 的 *sink* 有时可以拿来处理废弃的消息从而避免日志中出现找不到 *sink* 的噪音
- *CachedProxy* 扩展组件通常用来当作远端配置同步器使用

# 组件和语义 (Components & Semantics)

组件是用于实现数据处理器的基础抽象, 而语义则是用来实现各类协议的基础抽象, 语义最终将使用 Scala 的 *蛋糕模式 (cake pattern)* 在声明组件类的定义时通过 ``extends ... with ... with ...`` 的方式方便的引入进最终的组件实现

目前内置的组件通过不同包和项目的形式存在, 这样做的目的是将基础核心包的依赖列表尽可能的减少. 当你在使用核心框架构建你自己的数据流时, 可以最大限度的减少本项目引入的依赖

| GroupID                    | ArtifactID           | Description             |
| :------------------------: | :------------------: | :---------------------: |
| com.typesafe               | config               | 配置子系统              |
| io.kamon                   | kamon-core           | 监控数据的收集          |
|                            | kamon-system-metrics | 监控数据的收集 - 系统级 |
| com.typesafe.akka          | akka-actor           | 核心 Actor 模型         |
| com.typesafe.scala-logging | scala-logging        | 日志子系统              |

## 内置的语义支持

除 *BatchSinkSemantics* 为核心项目内置外, 其它分不同项目支持了如下几种语义

|                   语义名称                   |      类型       |              用途               |
| :------------------------------------------: | :-------------: | :-----------------------------: |
|        atiesh.sink.BatchSinkSemantics        |  SinkSemantics  |           批处理维护            |
|        atiesh.sink.HttpSinkSemantics         |  SinkSemantics  |     协议支持 - Http Client      |
|  atiesh.sink.HttpLimitRequestSinkSemantics   |  SinkSemantics  |     协议支持 - Http Client      |
|      atiesh.source.HttpSourceSemantics       | SourceSemantics |     协议支持 - Http Server      |
|        atiesh.sink.KafkaSinkSemantics        |  SinkSemantics  |    协议支持 - Kafka Producer    |
|    atiesh.sink.KafkaLimitAckSinkSemantics    |  SinkSemantics  |    协议支持 - Kafka Producer    |
| atiesh.sink.KafkaSynchronousAckSinkSemantics |  SinkSemantics  |    协议支持 - Kafka Producer    |
|      atiesh.source.KafkaSourceSemantics      | SourceSemantics |    协议支持 - Kafka Consumer    |
| atiesh.source.DirectoryWatchSourceSemantics  | SourceSemantics |   协议支持 - 文件系统目录监听   |
|       atiesh.sink.SyslogSinkSemantics        |  SinkSemantics  |        协议支持 - Syslog        |
|      atiesh.sink.AliyunSLSSinkSemantics      |  SinkSemantics  | 协议支持 -  Aliyun SLS Producer |

- `HttpLimitRequestSinkSemantics` 基于 `HttpSinkSemantics` 实现, 充当 *HighLevel API*
- `KafkaLimitAckSinkSemantics` 和 `KafkaSynchronousAckSinkSemantics` 基于 `KafkaSinkSemantics` 实现, 充当 *HighLevel API*

## API 层级

默认情况下, 涉及语义的组件包括 *source* 和 *sink* 两大类, 而他们的接口分为三层

- *Component API*, 组件接口, 定义了最基本的启动和停止以及消息处理的 *hooks* , 用于实现最终组件
- *Semantics API*, 语义接口, 定义了和 *akka-actor* 以及 *backpressure/start/stop* 事务交互的内部 *hooks* , 用于实现语义层 (所有内置的语义实现都是基于这个层面实现, 并且他们实际上都是 Scala 的 `Promise`)
- *Internal API*, 内部接口, 定义了最基本的 *akka-actor* 实现以及组件的基础语义, 原则上你只能拿它参考有哪些语义而不是去在这个层面做改动

## 组件配置示例

假设你想构建一个读取 KafkaTopic `incoming-channel` 并记录每一条读到的消息然后再丢弃它们

```
akka { ... }
kamon { ... }

atiesh {
    # 组件块 - 声明 Source 组件
    source {
        # 声明一个 source 组件, 满足如下要求:
        #   组件名 - <kafka-consumer>
        #   数据获取类 - <atiesh.source.KafkaSource>
        #   数据拦截器组件 - <records-logger>
        #   数据输出组件 - <devnull>
        #   标准 Kafka Consumer Properties - <group.id = cg-atiesh, ...>
        kafka-consumer {
            fqcn = "atiesh.source.KafkaSource"

            interceptors = ["records-logger"]
            sinks = ["devnull"]

            topics = ["incoming-channel"]
            poll-timeout = 1000 ms

            kafka-properties {
                "group.id" = "cg-atiesh"
                "bootstrap.servers" = "kafka-broker-01.0ops.io:9092,kafka-broker-02.0ops.io:9092,..."
                "enable.auto.commit" = true
                "auto.commit.interval.ms" = 30000
                "session.timeout.ms" = 30000
                "key.deserializer" = "org.apache.kafka.common.serialization.StringDeserializer"
                "value.deserializer" = "org.apache.kafka.common.serialization.StringDeserializer"
                ...
            }
        }
    }

    # 组件块 - 声明 Interceptor 组件
    interceptor {
        # 声明一个 interceptor 组件, 满足如下要求:
        #   组件名 - <records-logger>
        #   数据拦截/处理类 - <atiesh.interceptor.Transparent>
        #   拦截器优先级 - <90>
        records-logger {
            fqcn = "atiesh.interceptor.Transparent"
            priority = 90
        }
    }

    # 组件块 - 声明 Sink 组件
    sink {
        # 声明一个 sink 组件, 满足如下要求:
        #   组件名 - <devnull>
        #   数据处理类 - <atiesh.sink.DevNull>
        devnull {
            fqcn = "atiesh.sink.DevNull"
        }
    }

    # 组件块 - 声明 Extension 组件
    extension {
        ...
    }
}
```

- Atiesh 服务将会按照配置声明构建所有需要的类, 并且通过连接它们来将它们组装成一个数据处理通道
- 组件构建顺序: extension => interceptor => sink => source
- 组件关闭顺序: source => sink => extension

# 许可

开源基于 MIT License

# 作者和维护者

Hao Feng < whisperaven@gmail.com >
