package parallel

import (
	"github.com/IBM/sarama"
	"github.com/airowner/kafka"
)

type ConsumerGroupHandler struct {
	handler kafka.ConsumerHandler
	config  Config
}

func NewConsumerGroupHandler(config Config, handler kafka.ConsumerHandler, middlewares ...kafka.HandlerMiddleware) *ConsumerGroupHandler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return &ConsumerGroupHandler{
		handler: handler,
		config:  config,
	}
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	committer := newCommitter(session, claim, h.handler, h.config)

	for {
		select {
		case <-session.Context().Done():
			committer.close()
			return nil
		case message := <-claim.Messages():
			committer.push(message)
		}
	}
}
