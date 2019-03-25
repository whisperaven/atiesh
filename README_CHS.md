# Atiesh

**埃提耶什 (Atiesh)** 是一个开源流处理开发框架, 基于 **akka-actor** 开发并内置简单可靠的背压 (backpressure) 机制

在魔兽世界中, 埃提耶什是艾泽拉斯守护者的法杖, 希望你能用它为你的数据流打开一个传送门, 就像麦迪文为兽人做的那样 :>

# 组件和事件 (Components & Events)

用于构建数据流的基础组件, 一共包含三种, 它们包括

- 来源 (Source)
- 拦截器 (Interceptor)
- 去向 (Sink)

而在它们之中传递的消息则被包装成事件 (Event) 当前默认等事件类为 *atiesh.event.SimpleEvent*

最后一种组件为支持组件 (e.g.: *atiesh.utils.http.CachedProxy*) 基于 *atiesh.utils.AtieshComponent* 开发

## 数据流的生命周期和单位

数据 (或者说消息) 的最小单位是 *event* , 它们被 *source* 从外部服务接收或读取并包装成 *event*, 经由给定的 *interceptor* 去做处理(数据清洗或过滤, 可按需丢弃), 最后将处理后的 *event* 发送给特定的 *sink* 并由这个 *sink* 将其加工发送给外部系统 (e.g.: 包装 Http Request, 批处理等) , 至此一条数据的处理完毕

## 内置组件列表

| 组件名称                       | 类型          | 用途                      |
| :----------------------------: | :-----------: | :-----------------------: |
| atiesh.source.DevZero          | Source        | 生成消息内容为 "0" 的消息 |
| atiesh.interceptor.DevNull     | Interceptor   | 拦截并丢弃所有消息        |
| atiesh.interceptor.Transparent | Interceptor   | 拦截并放行所有消息        |
| atiesh.sink.DevNull            | Sink          | 接收并丢弃所有消息        |
| atiesh.utils.http.CachedProxy  | UtilComponent | 外部 Http 访问 & 缓存工具 |

- 这些组件通常用于测试以及当作简单的组件开发示例 ( *CachedProxy* 除外)
- *DevNull* 的 *sink* 有时可以拿来处理废弃的消息从而避免日志中出现找不到 *sink* 的噪音

# 组件和语义 (Components & Semantics)

组件是用于实现数据处理器的基础抽象, 而语义则是用来实现各类协议的基础抽象, 语义最终将使用 Scala 的 *蛋糕模式 (cake pattern)* 在声明组件类的定义时通过 ``extends ... with ... with ...`` 的方式方便的引入进来

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

| 语义名称                           | 类型            | 用途                      |
| :--------------------------------: | :-------------: | :-----------------------: |
| atiesh.sink.BatchSinkSemantics     | SinkSemantics   | 批处理维护                |
| atiesh.source.KafkaSourceSemantics | SourceSemantics | 协议支持 - Kafka Consumer |
| atiesh.sink.HttpSinkSemantics      | SinkSemantics   | 协议支持 - Http           |
| atiesh.sink.SyslogSinkSemantics    | SinkSemantics   | 协议支持 - Syslog         |

## API 层级

默认情况下, 涉及语义的组件包括 *source* 和 *sink* 两大类, 而他们的接口分为三层

- *Component API*, 组件接口, 定义了最基本的启动和停止以及消息处理的 *hooks* , 用于实现最终组件
- *Semantics API*, 语义接口, 定义了和 *akka-actor* 以及 *backpressure* 事务交互的内部 *hooks* , 用于实现语义层 (所有内置的语义实现都是基于这个层面实现)
- *Internal API*, 内部接口, 定义了最基本的 *akka-actor* 实现以及组件的基础语义, 原则上你只能拿它参考有哪些语义而不是去在这个层面做改动

# 许可

开源基于 MIT License

# 作者和维护者

Hao Feng < whisperaven@gmail.com >
