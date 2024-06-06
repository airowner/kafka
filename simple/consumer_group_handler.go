package simple

import (
	"github.com/IBM/sarama"
	"github.com/airowner/kafka"
)

type ConsumerGroupHandler struct {
	handler kafka.ConsumerHandler
}

func NewConsumerGroupHandler(handler kafka.ConsumerHandler, middlewares ...kafka.HandlerMiddleware) *ConsumerGroupHandler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return &ConsumerGroupHandler{
		handler: handler,
	}
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		select {
		case <-session.Context().Done():
			return nil
		case message := <-claim.Messages():
			err := h.handler(session.Context(), session, message)
			if err != nil {
				panic(err)
			}
		}
	}
}
