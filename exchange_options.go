package xamqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// ExchangeOptions 交换机配置选项。
//
// Passive=true 时，客户端只检查交换机是否存在及参数是否匹配，
// 若交换机不存在则返回错误，不会自动创建，适合验证已有基础设施的配置一致性。
type ExchangeOptions struct {
	Name       string     // 交换机名称
	Kind       string     // 交换机类型：""（默认直连）、direct、topic、fanout、headers
	Durable    bool       // 持久化：RabbitMQ 重启后交换机依然存在
	AutoDelete bool       // 自动删除：最后一个绑定移除后自动删除交换机
	Internal   bool       // 内部交换机：只能由其他交换机路由，不接受客户端直接发布
	NoWait     bool       // 不等待服务器确认：声明操作异步执行，不等待响应
	Passive    bool       // 被动模式：仅检查是否存在，不创建
	Args       amqp.Table // 额外参数（如 x-dead-letter-exchange 等）
	Declare    bool       // 是否在启动时声明：false 表示跳过声明，连接到已有交换机
	Bindings   []Binding  // 与此交换机关联的队列绑定列表
}
