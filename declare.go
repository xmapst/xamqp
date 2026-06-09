package xamqp

import (
	"errors"

	"github.com/xmapst/xamqp/internal/manager/channel"
)

// Declarator 用于声明 RabbitMQ 的交换机、队列和绑定关系。
//
// 将资源声明操作从业务的 Publisher/Consumer 中分离，
// 适合在应用启动时统一完成 AMQP 拓扑的初始化，
// 避免每个 Publisher/Consumer 都重复执行声明逻辑。
type Declarator struct {
	chanManager *channel.Manager
}

// NewDeclarator 创建资源声明器，使用独立的 AMQP 通道。
func NewDeclarator(conn *Conn, optionFuncs ...func(*DeclareOptions)) (*Declarator, error) {
	options := new(getDefaultDeclareOptions())
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connManager == nil {
		return nil, errors.New("connection manager can't be nil")
	}

	chanManager, err := channel.New(conn.connManager, options.Logger, conn.connManager.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	result := &Declarator{
		chanManager: chanManager,
	}

	return result, nil
}

// Close 关闭声明器，释放底层 AMQP 通道。
func (d *Declarator) Close() {
	_ = d.chanManager.Close()
}

// Exchange 声明交换机，若交换机已存在且参数相同则为幂等操作。
func (d *Declarator) Exchange(options ExchangeOptions) error {
	return declareExchange(d.chanManager, options)
}

// Queue 声明队列，若队列已存在且参数相同则为幂等操作。
func (d *Declarator) Queue(options QueueOptions) error {
	return declareQueue(d.chanManager, options)
}

// BindExchanges 创建交换机之间的绑定关系（Exchange-to-Exchange 路由）。
//
// E2E 绑定允许消息从源交换机路由到目标交换机，
// 实现更复杂的消息路由拓扑（如扇出后再过滤）。
func (d *Declarator) BindExchanges(bindings []Binding) error {
	for _, binding := range bindings {
		err := d.chanManager.ExchangeBindSafe(
			binding.Destination,
			binding.RoutingKey,
			binding.Source,
			binding.NoWait,
			binding.Args,
		)

		if err != nil {
			return err
		}
	}

	return nil
}

// BindQueues 创建交换机到队列的绑定关系。
//
// 绑定关系决定消息从交换机路由到哪些队列，
// 路由键的含义取决于交换机类型（direct/topic/fanout/headers）。
func (d *Declarator) BindQueues(bindings []Binding) error {
	for _, binding := range bindings {
		err := d.chanManager.QueueBindSafe(
			binding.Destination,
			binding.RoutingKey,
			binding.Source,
			binding.NoWait,
			binding.Args,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// declareQueue 声明队列，根据 Passive 标志选择主动声明或被动检查。
//
// Passive=true：仅检查队列是否存在，不创建，用于验证配置一致性。
// Passive=false：若不存在则创建，若存在则验证参数一致性。
// Declare=false：跳过声明，用于连接已知存在的外部队列。
func declareQueue(chanManager *channel.Manager, options QueueOptions) error {
	if !options.Declare {
		return nil
	}
	if options.Passive {
		_, err := chanManager.QueueDeclarePassiveSafe(
			options.Name,
			options.Durable,
			options.AutoDelete,
			options.Exclusive,
			options.NoWait,
			options.Args,
		)
		if err != nil {
			return err
		}
		return nil
	}
	_, err := chanManager.QueueDeclareSafe(
		options.Name,
		options.Durable,
		options.AutoDelete,
		options.Exclusive,
		options.NoWait,
		options.Args,
	)
	if err != nil {
		return err
	}
	return nil
}

// declareExchange 声明交换机，根据 Passive 标志选择主动声明或被动检查。
func declareExchange(chanManager *channel.Manager, options ExchangeOptions) error {
	if !options.Declare {
		return nil
	}
	if options.Passive {
		err := chanManager.ExchangeDeclarePassiveSafe(
			options.Name,
			options.Kind,
			options.Durable,
			options.AutoDelete,
			options.Internal,
			options.NoWait,
			options.Args,
		)
		if err != nil {
			return err
		}
		return nil
	}
	err := chanManager.ExchangeDeclareSafe(
		options.Name,
		options.Kind,
		options.Durable,
		options.AutoDelete,
		options.Internal,
		options.NoWait,
		options.Args,
	)
	if err != nil {
		return err
	}
	return nil
}

// declareBindings 为消费者选项中配置的所有交换机声明队列绑定关系。
//
// 仅声明 Declare=true 的绑定，跳过 Declare=false 的绑定（用于连接已有绑定）。
func declareBindings(chanManager *channel.Manager, options ConsumerOptions) error {
	for _, exchangeOption := range options.ExchangeOptions {
		for _, binding := range exchangeOption.Bindings {
			if !binding.Declare {
				continue
			}
			err := chanManager.QueueBindSafe(
				options.QueueOptions.Name,
				binding.RoutingKey,
				exchangeOption.Name,
				binding.NoWait,
				binding.Args,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
