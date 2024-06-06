package kafka

import (
	"github.com/IBM/sarama"
)

type MessageMiddleware func(*sarama.ProducerMessage) *sarama.ProducerMessage

type SyncProducer struct {
	sarama.SyncProducer
	middlewares []MessageMiddleware
}

func NewSyncProducer(client sarama.Client, middlewares ...MessageMiddleware) (*SyncProducer, error) {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	reverseSlice(middlewares)

	return &SyncProducer{
		SyncProducer: producer,
		middlewares:  middlewares,
	}, nil
}

func (sp *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	for _, middleware := range sp.middlewares {
		msg = middleware(msg)
	}
	return sp.SyncProducer.SendMessage(msg)
}

func (sp *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		for _, middleware := range sp.middlewares {
			msg = middleware(msg)
		}
	}
	return sp.SyncProducer.SendMessages(msgs)
}
