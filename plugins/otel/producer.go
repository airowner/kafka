package otel

import (
	"github.com/IBM/sarama"
	"github.com/airowner/kafka"
	"github.com/dnwe/otelsarama"
)

func NewSyncProducer(client sarama.Client, middlewares ...kafka.MessageMiddleware) (sarama.SyncProducer, error) {
	sp, err := kafka.NewSyncProducer(client, middlewares...)
	if err != nil {
		return nil, err
	}
	sp.SyncProducer = otelsarama.WrapSyncProducer(client.Config(), sp.SyncProducer)

	return sp, nil
}
