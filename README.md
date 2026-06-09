# xamqp

`xamqp` 是一个面向 Go 的 RabbitMQ 客户端增强封装，基于 [`github.com/rabbitmq/amqp091-go`](go.mod:5) 实现，重点解决原生 AMQP 使用中常见的工程化问题：

- 连接与通道异常后的自动重连
- `Publisher` / `Consumer` / `Declarator` 等高层抽象
- 更清晰的选项式 API
- 发布确认、退回消息、流控与阻塞信号处理
- 队列 / 交换机 / 绑定关系的自动声明
- 优雅关闭与消费中消息处理完成等待

该仓库适合需要在生产环境中稳定使用 RabbitMQ 的 Go 服务，尤其适合希望避免直接重复编写连接恢复、通道恢复、拓扑声明和消费确认样板代码的场景。

---

## 目录

- [特性概览](#特性概览)
- [安装](#安装)
- [设计目标](#设计目标)
- [核心组件](#核心组件)
- [快速开始](#快速开始)
  - [1. 建立连接](#1-建立连接)
  - [2. 声明交换机、队列与绑定](#2-声明交换机队列与绑定)
  - [3. 创建消费者](#3-创建消费者)
  - [4. 创建发布者](#4-创建发布者)
- [API 说明](#api-说明)
  - [连接 `Conn`](#连接-conn)
  - [底层通道 `Channel`](#底层通道-channel)
  - [声明器 `Declarator`](#声明器-declarator)
  - [消费者 `Consumer`](#消费者-consumer)
  - [发布者 `Publisher`](#发布者-publisher)
- [配置项详解](#配置项详解)
  - [连接配置](#连接配置)
  - [通道配置](#通道配置)
  - [消费者配置](#消费者配置)
  - [发布者配置](#发布者配置)
  - [单次发布配置](#单次发布配置)
- [消息确认与消费语义](#消息确认与消费语义)
- [自动重连机制](#自动重连机制)
- [拓扑声明策略](#拓扑声明策略)
- [日志](#日志)
- [适用场景建议](#适用场景建议)
- [注意事项](#注意事项)
- [许可证](#许可证)

---

## 特性概览

### 1. 自动重连

[`Conn`](connection.go:16) 基于内部连接管理器封装了 RabbitMQ 连接生命周期；[`Consumer`](consume.go:40)、[`Publisher`](publish.go:51)、[`Channel`](channel.go:14) 基于通道管理器继续实现通道级恢复。当连接或通道异常关闭时，库会自动进行指数退避重连。

- 初始退避由 [`ConnectionOptions.ReconnectInterval`](connection_options.go:9) 控制
- 失败后按指数退避增长
- 最大退避时间为 60 秒，见 [`reconnectLoop()`](internal/manager/connection/connection.go:162) 与 [`reconnectLoop()`](internal/manager/channel/channel.go:126)

### 2. 高层抽象

仓库提供几个主要入口：

- [`NewConn()`](connection.go:59)：创建单节点连接
- [`NewClusterConn()`](connection.go:67)：创建集群连接，支持自定义解析器
- [`NewDeclarator()`](declare.go:19)：统一声明交换机、队列和绑定
- [`NewConsumer()`](consume.go:61)：创建自动恢复的消费者
- [`NewPublisher()`](publish.go:80)：创建自动恢复的发布者
- [`NewChannel()`](channel.go:22)：暴露底层 AMQP 通道能力

### 3. 面向生产环境的行为细节

- 消费者支持并发消费，见 [`ConsumerOptions.Concurrency`](consumer_options.go:75)
- 支持优雅关闭，见 [`CloseWithContext()`](consume.go:181)
- 支持发布确认，见 [`NotifyPublish()`](publish.go:323)
- 支持 `mandatory` 退回处理，见 [`NotifyReturn()`](publish.go:307)
- 支持 RabbitMQ `Flow` 与连接 `Blocking` 背压控制，见 [`startNotifyFlowHandler()`](publish_flow_block.go:13) 与 [`startNotifyBlockedHandler()`](publish_flow_block.go:44)
- 支持队列参数透传，如 `x-message-ttl`、`x-expires`、`x-queue-type=quorum`

---

## 安装

要求：

- Go 版本：项目当前声明为 [`go 1.26.3`](go.mod:3)
- RabbitMQ Go 客户端依赖：[`github.com/rabbitmq/amqp091-go`](go.mod:5)

安装命令：

```bash
go get github.com/xmapst/xamqp
```

---

## 设计目标

`xamqp` 并不是试图替代原生 AMQP 概念，而是在尽量保留 RabbitMQ 使用模型的前提下，解决以下问题：

1. 原生连接 / 通道在网络抖动或 Broker 重启后需要业务代码手动恢复。
2. 业务代码经常重复编写交换机、队列、绑定声明逻辑。
3. 消费确认、并发处理、优雅关闭容易出现遗漏。
4. 发布确认、退回消息、流控处理通常需要额外样板代码。
5. 多个模块共享同一连接时，重连与阻塞通知管理容易复杂化。

该仓库通过内部 [`connection.Manager`](internal/manager/connection/connection.go:27)、[`channel.Manager`](internal/manager/channel/channel.go:25) 和 [`dispatcher.Dispatcher`](internal/dispatcher/dispatcher.go:17) 完成连接恢复与事件广播，使上层 API 保持相对简洁。

---

## 核心组件

### [`Conn`](connection.go:16)

用于管理与 RabbitMQ 的连接，可在多个发布者和消费者之间共享。

能力：

- 支持单节点或集群连接
- 支持自定义地址解析器 [`IResolver`](connection.go:31)
- 支持静态地址列表解析器 [`StaticResolver`](connection.go:37)
- 支持连接参数透传 [`Config`](connection.go:28)
- 自动处理断线重连

### [`Declarator`](declare.go:14)

用于在应用启动时集中声明 RabbitMQ 拓扑资源。

能力：

- 声明交换机
- 声明队列
- 绑定交换机到交换机
- 绑定交换机到队列

### [`Consumer`](consume.go:40)

用于持续消费消息，并在通道异常时自动恢复。

能力：

- 自动声明交换机 / 队列 / 绑定
- 并发消费
- 支持手动确认 / 自动确认 / 拒绝重入队 / 拒绝丢弃
- 支持优雅关闭
- 支持 panic 保护

### [`Publisher`](publish.go:51)

用于发布消息，并在重连后恢复发布能力与通知监听。

能力：

- 支持单次多路由键发布
- 支持发布确认
- 支持退回消息监听
- 支持 `Flow` / `Blocking` 背压控制
- 支持延迟确认对象 [`PublishWithDeferredConfirmWithContext()`](publish.go:226)

### [`Channel`](channel.go:14)

适合需要直接访问 AMQP 通道原语的高级场景。

能力：

- 暴露内部通道管理器
- 创建时自动应用 QoS
- 在需要直接调用底层安全包装方法时使用

---

## 快速开始

下面给出一个典型的“连接 → 声明 → 消费 → 发布”流程。

### 1. 建立连接

```go
package main

import (
    "log"
    "time"

    "github.com/xmapst/xamqp"
)

func main() {
    conn, err := xamqp.NewConn(
        "amqp://guest:guest@127.0.0.1:5672/",
        xamqp.WithConnectionOptionsReconnectInterval(3*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    _ = conn
}
```

若是 RabbitMQ 集群，可使用 [`NewClusterConn()`](connection.go:67)：

```go
resolver := xamqp.NewStaticResolver([]string{
    "amqp://guest:guest@node1:5672/",
    "amqp://guest:guest@node2:5672/",
    "amqp://guest:guest@node3:5672/",
}, true)

conn, err := xamqp.NewClusterConn(resolver)
```

其中 [`NewStaticResolver()`](connection.go:54) 的第二个参数 `shuffle=true` 表示每次解析时打乱节点顺序，可用于简单的客户端侧负载分散。

### 2. 声明交换机、队列与绑定

```go
package main

import (
    "log"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/xmapst/xamqp"
)

func declare(conn *xamqp.Conn) {
    declarator, err := xamqp.NewDeclarator(conn)
    if err != nil {
        log.Fatal(err)
    }
    defer declarator.Close()

    err = declarator.Exchange(xamqp.ExchangeOptions{
        Name:    "orders.exchange",
        Kind:    amqp.ExchangeDirect,
        Durable: true,
        Declare: true,
    })
    if err != nil {
        log.Fatal(err)
    }

    err = declarator.Queue(xamqp.QueueOptions{
        Name:    "orders.created",
        Durable: true,
        Declare: true,
    })
    if err != nil {
        log.Fatal(err)
    }

    err = declarator.BindQueues([]xamqp.Binding{
        {
            Source:      "orders.exchange",
            Destination: "orders.created",
            RoutingKey:  "orders.created",
            BindingOptions: xamqp.BindingOptions{
                Declare: true,
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### 3. 创建消费者

```go
package main

import (
    "log"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/xmapst/xamqp"
)

func startConsumer(conn *xamqp.Conn) {
    consumer, err := xamqp.NewConsumer(
        conn,
        "orders.created",
        xamqp.WithConsumerOptionsQueueDurable,
        xamqp.WithConsumerOptionsExchangeName("orders.exchange"),
        xamqp.WithConsumerOptionsExchangeKind(amqp.ExchangeDirect),
        xamqp.WithConsumerOptionsExchangeDeclare,
        xamqp.WithConsumerOptionsRoutingKey("orders.created"),
        xamqp.WithConsumerOptionsConcurrency(4),
        xamqp.WithConsumerOptionsQOSPrefetch(20),
    )
    if err != nil {
        log.Fatal(err)
    }

    err = consumer.Run(func(d xamqp.Delivery) xamqp.Action {
        log.Printf("received: %s", string(d.Body))
        return xamqp.Ack
    })
    if err != nil {
        log.Fatal(err)
    }

    defer consumer.Close()
}
```

消费处理函数返回 [`Action`](consume.go:16) 来表达确认语义：

- [`Ack`](consume.go:23)：成功处理，确认消息
- [`NackDiscard`](consume.go:26)：拒绝并丢弃 / 进入死信
- [`NackRequeue`](consume.go:29)：拒绝并重新入队
- [`Manual`](consume.go:32)：自行调用 [`Delivery.Ack()`](consume.go:52) / `Nack()` / `Reject()`

### 4. 创建发布者

```go
package main

import (
    "context"
    "log"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/xmapst/xamqp"
)

func publish(conn *xamqp.Conn) {
    publisher, err := xamqp.NewPublisher(
        conn,
        xamqp.WithPublisherOptionsExchangeName("orders.exchange"),
        xamqp.WithPublisherOptionsExchangeKind(amqp.ExchangeDirect),
        xamqp.WithPublisherOptionsExchangeDeclare,
        xamqp.WithPublisherOptionsExchangeDurable,
        xamqp.WithPublisherOptionsConfirm,
    )
    if err != nil {
        log.Fatal(err)
    }
    defer publisher.Close()

    publisher.NotifyPublish(func(c xamqp.Confirmation) {
        log.Printf("ack=%v tag=%d reconnect=%d", c.Ack, c.DeliveryTag, c.ReconnectionCount)
    })

    publisher.NotifyReturn(func(r xamqp.Return) {
        log.Printf("returned: replyCode=%d replyText=%s routingKey=%s", r.ReplyCode, r.ReplyText, r.RoutingKey)
    })

    err = publisher.PublishWithContext(
        context.Background(),
        []byte(`{"order_id":1001}`),
        []string{"orders.created"},
        xamqp.WithPublishOptionsExchange("orders.exchange"),
        xamqp.WithPublishOptionsContentType("application/json"),
        xamqp.WithPublishOptionsPersistentDelivery,
        xamqp.WithPublishOptionsMandatory,
        xamqp.WithPublishOptionsTimestamp(time.Now()),
    )
    if err != nil {
        log.Fatal(err)
    }
}
```

如果希望等待单条或多条消息的 Broker 确认，可使用 [`PublishWithDeferredConfirmWithContext()`](publish.go:226)：

```go
confs, err := publisher.PublishWithDeferredConfirmWithContext(
    context.Background(),
    []byte("hello"),
    []string{"orders.created"},
    xamqp.WithPublishOptionsExchange("orders.exchange"),
)
if err != nil {
    log.Fatal(err)
}

for _, conf := range confs {
    if conf == nil {
        continue
    }
    confirmed := conf.Wait()
    log.Printf("confirmed=%v ack=%v", confirmed, conf.Acked())
}
```

---

## API 说明

## 连接 `Conn`

主要入口：

- [`NewConn()`](connection.go:59)
- [`NewClusterConn()`](connection.go:67)
- [`NewStaticResolver()`](connection.go:54)
- [`Close()`](connection.go:99)
- [`IsClosed()`](connection.go:105)

说明：

- [`NewConn()`](connection.go:59) 适合单个 RabbitMQ 节点地址。
- [`NewClusterConn()`](connection.go:67) 支持传入自定义 [`IResolver`](connection.go:31)，可以自行实现轮询、随机、权重、服务发现等策略。
- [`Config`](connection.go:28) 是对 `amqp.Config` 的别名封装，可传入心跳、最大通道数、帧大小等连接协商参数。

### 示例：自定义解析器

```go
type MyResolver struct{}

func (r *MyResolver) Resolve() ([]string, error) {
    return []string{
        "amqp://guest:guest@mq-1:5672/",
        "amqp://guest:guest@mq-2:5672/",
    }, nil
}
```

将其传给 [`NewClusterConn()`](connection.go:67) 即可。

---

## 底层通道 `Channel`

入口：[`NewChannel()`](channel.go:22)

适用场景：

- 你需要直接调用底层通道方法
- 你不希望使用高层 `Publisher` / `Consumer` 抽象
- 你需要自定义更底层的 AMQP 操作

创建时会先应用 [`ChannelOptions.QOSPrefetch`](channel_options.go:6) 与 [`ChannelOptions.QOSGlobal`](channel_options.go:7)，如果 QoS 设置失败会直接返回错误，避免通道带着错误配置继续被使用，见 [`NewChannel()`](channel.go:22)。

---

## 声明器 `Declarator`

入口：[`NewDeclarator()`](declare.go:19)

支持的方法：

- [`Exchange()`](declare.go:47)
- [`Queue()`](declare.go:52)
- [`BindExchanges()`](declare.go:60)
- [`BindQueues()`](declare.go:82)
- [`Close()`](declare.go:42)

建议：

- 在服务启动阶段使用单独的声明器统一初始化拓扑
- 业务消费与发布过程中尽量避免重复声明，以减少拓扑漂移和启动噪音
- 如果资源由平台统一预建，可结合 `Passive=true` 或 `Declare=false`

---

## 消费者 `Consumer`

入口：

- [`NewConsumer()`](consume.go:61)
- [`Run()`](consume.go:103)
- [`Close()`](consume.go:156)
- [`CloseWithContext()`](consume.go:181)
- [`IsClosed()`](consume.go:253)

### 消费流程

[`Run()`](consume.go:103) 内部会：

1. 设置 QoS
2. 声明交换机
3. 声明队列
4. 声明绑定
5. 调用 `basic.consume`
6. 按 [`ConsumerOptions.Concurrency`](consumer_options.go:75) 启动多个处理 goroutine

### panic 处理

消费 handler 中如果发生 panic，库会在 [`handlerWrapper()`](consume.go:282) 中恢复，记录错误日志，并在非自动确认模式下对当前消息执行 `Nack(requeue=false)`，防止毒丸消息不断触发消费者崩溃。

### 优雅关闭

默认情况下 [`ConsumerOptions.CloseGracefully`](consumer_options.go:73) 为 `true`。关闭时：

- 不再接收新消息
- 等待当前处理中的消息完成
- 然后关闭通道与连接管理订阅

如果希望快速退出，可使用 [`WithConsumerOptionsForceShutdown()`](consumer_options.go:347)。

---

## 发布者 `Publisher`

入口：

- [`NewPublisher()`](publish.go:80)
- [`Publish()`](publish.go:150)
- [`PublishWithContext()`](publish.go:163)
- [`PublishWithDeferredConfirmWithContext()`](publish.go:226)
- [`NotifyReturn()`](publish.go:307)
- [`NotifyPublish()`](publish.go:323)
- [`Close()`](publish.go:290)

### 发布行为

[`PublishWithContext()`](publish.go:163) 支持：

- 一次向多个路由键发布同一条消息
- 为每个路由键单独调用 AMQP 发布
- 发布前检查 `Flow` 与 `Blocking` 状态
- 将 `PublishOptions` 映射到 `amqp.Publishing`

### 背压与流控

当 RabbitMQ 服务器压力较大时：

- [`startNotifyFlowHandler()`](publish_flow_block.go:13) 会接收服务器 `Flow` 信号
- [`startNotifyBlockedHandler()`](publish_flow_block.go:44) 会接收连接 `Blocking` 信号

此时发布者会临时拒绝新的发布请求，并返回错误：

- `publishing blocked due to high flow on the server`
- `publishing blocked due to TCP block on the server`

调用方应在业务侧做重试、退避或熔断。

### 发布确认

启用 [`WithPublisherOptionsConfirm()`](publisher_options.go:105) 后，可通过 [`NotifyPublish()`](publish.go:323) 监听确认结果。

确认结构体 [`Confirmation`](publish.go:38) 额外带有 [`ReconnectionCount`](publish.go:40)，用于区分重连前后的投递标签范围，避免只依赖 `DeliveryTag` 造成歧义。

---

## 配置项详解

## 连接配置

结构体：[`ConnectionOptions`](connection_options.go:8)

字段：

- [`ReconnectInterval`](connection_options.go:9)：重连退避的初始等待时间
- [`Logger`](connection_options.go:10)：自定义日志实现
- [`Config`](connection_options.go:11)：AMQP 连接协商参数

可选函数：

- [`WithConnectionOptionsReconnectInterval()`](connection_options.go:26)
- [`WithConnectionOptionsLogging()`](connection_options.go:33)
- [`WithConnectionOptionsLogger()`](connection_options.go:38)
- [`WithConnectionOptionsConfig()`](connection_options.go:47)

---

## 通道配置

结构体：[`ChannelOptions`](channel_options.go:4)

字段：

- [`Logger`](channel_options.go:5)
- [`QOSPrefetch`](channel_options.go:6)
- [`QOSGlobal`](channel_options.go:7)
- [`ConfirmMode`](channel_options.go:8)

可选函数：

- [`WithChannelOptionsQOSPrefetch()`](channel_options.go:24)
- [`WithChannelOptionsQOSGlobal()`](channel_options.go:34)
- [`WithChannelOptionsLogging()`](channel_options.go:39)
- [`WithChannelOptionsLogger()`](channel_options.go:44)
- [`WithChannelOptionsConfirm()`](channel_options.go:54)

说明：目前 [`ChannelOptions.ConfirmMode`](channel_options.go:8) 在 [`NewChannel()`](channel.go:22) 中未实际启用确认模式，若需要确认能力，推荐直接使用 [`Publisher`](publish.go:51)。

---

## 消费者配置

结构体：[`ConsumerOptions`](consumer_options.go:70)

关键字段：

- [`RabbitConsumerOptions`](consumer_options.go:71)：AMQP 原生消费参数
- [`QueueOptions`](consumer_options.go:72)：队列声明参数
- [`CloseGracefully`](consumer_options.go:73)：是否优雅关闭
- [`ExchangeOptions`](consumer_options.go:74)：交换机及绑定声明配置
- [`Concurrency`](consumer_options.go:75)：并发处理数
- [`Logger`](consumer_options.go:76)
- [`QOSPrefetch`](consumer_options.go:77)
- [`QOSGlobal`](consumer_options.go:78)

### 队列相关选项

- [`WithConsumerOptionsQueueDurable()`](consumer_options.go:122)
- [`WithConsumerOptionsQueueAutoDelete()`](consumer_options.go:127)
- [`WithConsumerOptionsQueueExpires()`](consumer_options.go:135)
- [`WithConsumerOptionsQueueExclusive()`](consumer_options.go:146)
- [`WithConsumerOptionsQueueNoWait()`](consumer_options.go:151)
- [`WithConsumerOptionsQueuePassive()`](consumer_options.go:156)
- [`WithConsumerOptionsQueueNoDeclare()`](consumer_options.go:161)
- [`WithConsumerOptionsQueueArgs()`](consumer_options.go:166)
- [`WithConsumerOptionsQueueQuorum()`](consumer_options.go:355)
- [`WithConsumerOptionsQueueMessageExpiration()`](consumer_options.go:368)

### 交换机相关选项

- [`WithConsumerOptionsExchangeName()`](consumer_options.go:183)
- [`WithConsumerOptionsExchangeKind()`](consumer_options.go:191)
- [`WithConsumerOptionsExchangeDurable()`](consumer_options.go:199)
- [`WithConsumerOptionsExchangeAutoDelete()`](consumer_options.go:205)
- [`WithConsumerOptionsExchangeInternal()`](consumer_options.go:211)
- [`WithConsumerOptionsExchangeNoWait()`](consumer_options.go:217)
- [`WithConsumerOptionsExchangeDeclare()`](consumer_options.go:223)
- [`WithConsumerOptionsExchangePassive()`](consumer_options.go:229)
- [`WithConsumerOptionsExchangeArgs()`](consumer_options.go:235)
- [`WithConsumerOptionsExchangeOptions()`](consumer_options.go:267)

### 绑定相关选项

- [`WithConsumerOptionsRoutingKey()`](consumer_options.go:246)
- [`WithConsumerOptionsBinding()`](consumer_options.go:259)

### 消费行为相关选项

- [`WithConsumerOptionsConcurrency()`](consumer_options.go:276)
- [`WithConsumerOptionsConsumerName()`](consumer_options.go:285)
- [`WithConsumerOptionsConsumerAutoAck()`](consumer_options.go:295)
- [`WithConsumerOptionsConsumerExclusive()`](consumer_options.go:305)
- [`WithConsumerOptionsConsumerNoWait()`](consumer_options.go:312)
- [`WithConsumerOptionsLogging()`](consumer_options.go:317)
- [`WithConsumerOptionsLogger()`](consumer_options.go:322)
- [`WithConsumerOptionsQOSPrefetch()`](consumer_options.go:332)
- [`WithConsumerOptionsQOSGlobal()`](consumer_options.go:339)
- [`WithConsumerOptionsForceShutdown()`](consumer_options.go:347)
- [`WithConsumerStreamOffset()`](consumer_options.go:380)

### 消费者默认值

默认构造见 [`getDefaultConsumerOptions()`](consumer_options.go:11)：

- 队列默认 `Declare=true`
- 交换机列表默认空
- `Concurrency=1`
- `CloseGracefully=true`
- `QOSPrefetch=10`
- `AutoAck=false`

---

## 发布者配置

结构体：[`PublisherOptions`](publisher_options.go:10)

字段：

- [`ExchangeOptions`](publisher_options.go:11)
- [`Logger`](publisher_options.go:12)
- [`ConfirmMode`](publisher_options.go:13)

可选函数：

- [`WithPublisherOptionsLogging()`](publisher_options.go:36)
- [`WithPublisherOptionsLogger()`](publisher_options.go:41)
- [`WithPublisherOptionsExchangeName()`](publisher_options.go:48)
- [`WithPublisherOptionsExchangeKind()`](publisher_options.go:55)
- [`WithPublisherOptionsExchangeDurable()`](publisher_options.go:62)
- [`WithPublisherOptionsExchangeAutoDelete()`](publisher_options.go:67)
- [`WithPublisherOptionsExchangeInternal()`](publisher_options.go:72)
- [`WithPublisherOptionsExchangeNoWait()`](publisher_options.go:77)
- [`WithPublisherOptionsExchangeDeclare()`](publisher_options.go:82)
- [`WithPublisherOptionsExchangePassive()`](publisher_options.go:87)
- [`WithPublisherOptionsExchangeArgs()`](publisher_options.go:92)
- [`WithPublisherOptionsConfirm()`](publisher_options.go:105)

默认值见 [`getDefaultPublisherOptions()`](publisher_options.go:17)：

- 默认交换机类型为 `direct`
- 默认不声明交换机
- 默认不开启确认模式

---

## 单次发布配置

结构体：[`PublishOptions`](publish_options.go:10)

常见选项函数：

- [`WithPublishOptionsExchange()`](publish_options.go:35)
- [`WithPublishOptionsMandatory()`](publish_options.go:42)
- [`WithPublishOptionsImmediate()`](publish_options.go:47)
- [`WithPublishOptionsContentType()`](publish_options.go:52)
- [`WithPublishOptionsPersistentDelivery()`](publish_options.go:63)
- [`WithPublishOptionsExpiration()`](publish_options.go:71)
- [`WithPublishOptionsHeaders()`](publish_options.go:78)
- [`WithPublishOptionsContentEncoding()`](publish_options.go:85)
- [`WithPublishOptionsPriority()`](publish_options.go:94)
- [`WithPublishOptionsCorrelationID()`](publish_options.go:101)
- [`WithPublishOptionsReplyTo()`](publish_options.go:108)
- [`WithPublishOptionsMessageID()`](publish_options.go:115)
- [`WithPublishOptionsTimestamp()`](publish_options.go:122)
- [`WithPublishOptionsType()`](publish_options.go:129)
- [`WithPublishOptionsUserID()`](publish_options.go:136)
- [`WithPublishOptionsAppID()`](publish_options.go:143)

说明：如果未显式设置投递模式，库会在 [`PublishWithContext()`](publish.go:163) 与 [`PublishWithDeferredConfirmWithContext()`](publish.go:226) 中默认使用 [`Transient`](publish.go:21)。

---

## 消息确认与消费语义

### 消费确认

处理函数类型为 [`Handler`](consume.go:19)：

```go
func(d xamqp.Delivery) xamqp.Action
```

返回动作：

- [`Ack`](consume.go:23)：调用 `msg.Ack(false)`
- [`NackDiscard`](consume.go:26)：调用 `msg.Nack(false, false)`
- [`NackRequeue`](consume.go:29)：调用 `msg.Nack(false, true)`
- [`Manual`](consume.go:32)：由业务自行操作确认

### 发布确认

发布确认有两种使用方式：

1. 事件通知方式：[`NotifyPublish()`](publish.go:323)
2. 延迟确认对象方式：[`PublishWithDeferredConfirmWithContext()`](publish.go:226)

推荐：

- 高吞吐、异步场景：使用 [`NotifyPublish()`](publish.go:323)
- 精确等待单条消息落盘确认：使用 [`PublishWithDeferredConfirmWithContext()`](publish.go:226)

---

## 自动重连机制

连接恢复链路：

- [`Conn`](connection.go:16) 持有内部 [`connection.Manager`](internal/manager/connection/connection.go:27)
- 连接异常关闭后，由 [`startNotifyClose()`](internal/manager/connection/connection.go:133) 触发重连
- 重连成功后，通过 [`dispatcher.Dispatcher`](internal/dispatcher/dispatcher.go:17) 向订阅者广播恢复事件
- 上层 [`Publisher`](publish.go:51)、[`Consumer`](consume.go:40) 和其他通道组件收到事件后重新初始化自身状态

通道恢复链路：

- [`channel.Manager`](internal/manager/channel/channel.go:25) 监听 `NotifyClose` / `NotifyCancel`
- 通道异常后触发内部重建
- 恢复成功后广播给通道使用者

这意味着：

- 连接断了时，无需业务手动重建 `Publisher` / `Consumer`
- 但业务仍应正确处理“当前一次操作失败”的错误返回
- 自动恢复并不等于操作幂等，消息去重仍应由业务设计保证

---

## 拓扑声明策略

本库对交换机 / 队列 / 绑定提供三种常见策略：

### 1. 启动时自动声明

适合应用自管理拓扑。

- 交换机设置 `Declare=true`
- 队列设置 `Declare=true`
- 绑定设置 `Declare=true`

### 2. 被动检查

适合平台已预建资源，只希望校验是否存在且参数一致。

- `Passive=true`
- 同时保留 `Declare=true`

例如 [`declareQueue()`](declare.go:103) 与 [`declareExchange()`](declare.go:136) 会在 `Passive=true` 时走被动检查分支。

### 3. 完全跳过声明

适合资源由外部系统统一维护，应用只消费 / 发布。

- `Declare=false`

例如：

- [`QueueOptions.Declare`](consumer_options.go:103)
- [`ExchangeOptions.Declare`](exchange_options.go:20)
- [`BindingOptions.Declare`](consumer_options.go:118)

---

## 日志

对外日志接口为 [`ILogger`](logger.go:14)，可通过多种 `With*Logger()` 选项注入。

默认实现为 [`stdDebugLogger`](logger.go:23)，基于 Go 标准库 `log` 输出，格式类似：

```text
gorabbit INFO: successful consumer recovery
gorabbit WARN: pausing publishing due to flow request from server
```

可注入的位置包括：

- [`WithConnectionOptionsLogger()`](connection_options.go:38)
- [`WithChannelOptionsLogger()`](channel_options.go:44)
- [`WithDeclareOptionsLogger()`](declare_options.go:23)
- [`WithConsumerOptionsLogger()`](consumer_options.go:322)
- [`WithPublisherOptionsLogger()`](publisher_options.go:41)

建议在生产环境中接入业务统一日志系统，便于关联链路、监控和告警。

---

## 适用场景建议

适合：

- 需要自动重连的 RabbitMQ 生产者 / 消费者服务
- 需要统一拓扑声明的微服务
- 对发布确认、退回消息、优雅关闭有要求的系统
- 希望保留 AMQP 语义但减少样板代码的 Go 项目

不那么适合：

- 只需一次性、极简的 AMQP 连接脚本
- 需要完全自定义底层恢复语义且不希望库介入生命周期管理的场景
- 非 RabbitMQ Broker 或对 `amqp091-go` 兼容性要求不同的环境

---

## 注意事项

1. [`PublishWithContext()`](publish.go:163) 在 Broker 流控或连接阻塞时会直接返回错误，调用方需要自行重试。
2. [`WithPublishOptionsImmediate()`](publish_options.go:47) 对应的 `Immediate` 标志在 RabbitMQ 3.x 中已废弃，使用前请确认 Broker 行为。
3. 消费者并发数高于 1 时，消息处理顺序不再严格等同于入队顺序。
4. [`WithConsumerOptionsConsumerAutoAck()`](consumer_options.go:295) 开启后，消息一投递即视为成功消费，处理失败无法依赖 AMQP 重试。
5. 使用持久化消息时，请同时考虑交换机、队列、消息三者的持久化组合策略。
6. 自动重连不保证业务层 exactly-once，需要结合幂等键、去重表或事务外盒模式。
7. [`WithConsumerStreamOffset()`](consumer_options.go:380) 用于 RabbitMQ Streams 相关参数透传，具体能力依赖 Broker 配置与队列类型。
8. 当前 README 基于仓库现有源码整理；若后续 API 发生变化，请以源码与 GoDoc 为准。

---

## 许可证

本项目采用 [`LICENSE`](LICENSE) 中定义的许可证。