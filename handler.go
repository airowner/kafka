package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

type ConsumerHandler func(context.Context, sarama.ConsumerGroupSession, *sarama.ConsumerMessage) error

type HandlerMiddleware func(c ConsumerHandler) ConsumerHandler
