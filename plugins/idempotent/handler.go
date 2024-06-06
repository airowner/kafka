package idempotent

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/airowner/kafka"
)

type HandlerMiddleware struct {
	Idempotent
}

func (m HandlerMiddleware) Handle(c kafka.ConsumerHandler) kafka.ConsumerHandler {
	return func(ctx context.Context, session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) (err error) {
		if m.Done(msg) {
			session.MarkMessage(msg, "")
		} else {
			err = c(ctx, session, msg)
			if err == nil {
				return m.SetDone(msg)
			}
		}
		return
	}
}
