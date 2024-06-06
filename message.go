package kafka

import (
	"github.com/IBM/sarama"
)

func NewMessage(topic, message string, key ...string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	if len(key) > 0 {
		msg.Key = sarama.StringEncoder(key[0])
	}

	return msg
}
