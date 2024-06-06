package otel

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/airowner/kafka"
	"github.com/dnwe/otelsarama"
	"go.opentelemetry.io/otel"
)

func NewMessage(ctx context.Context, topic, message string, key ...string) *sarama.ProducerMessage {
	msg := kafka.NewMessage(topic, message, key...)
	otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(msg))
	return msg
}
