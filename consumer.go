package kafka

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
)

type Consumer struct {
	client  sarama.Client
	groupId string
	topics  []string

	handler sarama.ConsumerGroupHandler
}

// NewConsumer 管理一个消费者组的生命周期
func NewConsumer(client sarama.Client, groupId string, topics []string, handler sarama.ConsumerGroupHandler) *Consumer {

	return &Consumer{
		client:  client,
		groupId: groupId,
		topics:  topics,
		handler: handler,
	}
}

// Start 启动消费
func (c *Consumer) Start(ctx context.Context) error {
	cg, err := sarama.NewConsumerGroupFromClient(c.groupId, c.client)
	if err != nil {
		return err
	}

	// 记录消费者错误信息
	go func() {
		for err := range cg.Errors() {
			if errors.Is(err, context.Canceled) {
				return
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			sarama.Logger.Printf("topic[%vs] group[%s], client_error: %s", c.topics, c.groupId, err)
		}
	}()

	// 开始消费
	stop := make(chan struct{})
	go func() {
		defer func() {
			if v := recover(); v != nil {
				sarama.Logger.Printf("panic: %v", v)
			}

			close(stop)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				e := cg.Consume(ctx, c.topics, c.handler)
				if e != nil {
					sarama.Logger.Printf("topic[%vs] group[%s], consume_error: %s", c.topics, c.groupId, err)

					var kerror sarama.KError
					if errors.Is(err, sarama.ErrClosedConsumerGroup) || errors.Is(err, &kerror) {
						return
					}
				}
				if ctx.Err() != nil {
					if ctx.Err() != context.Canceled {
						sarama.Logger.Printf("topic[%v] group[%s], ctx_error: %s", c.topics, c.groupId, ctx.Err())
					}
					return
				}
			}
		}
	}()

	sarama.Logger.Printf("consumer up and running!")

	// hold run
	<-stop

	// 客户端关闭
	if err = cg.Close(); err != nil {
		sarama.Logger.Printf("topic[%v] group[%s], close_error: %s", c.topics, c.groupId, err)
		return err
	}

	sarama.Logger.Printf("topic[%v] group[%s] safe exit ok", c.topics, c.groupId)

	return nil
}
