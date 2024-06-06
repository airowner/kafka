package otel

import (
	"context"
	"fmt"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/airowner/kafka"
	"github.com/dnwe/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type HandlerMiddleware struct {
	name string
}

func (h HandlerMiddleware) Handle(c kafka.ConsumerHandler) kafka.ConsumerHandler {
	propagator := otel.GetTextMapPropagator()
	tracer := otel.GetTracerProvider().Tracer(h.name, trace.WithInstrumentationVersion("v1"))
	return func(ctx context.Context, session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) (err error) {
		carrier := otelsarama.NewConsumerMessageCarrier(message)
		ctx = propagator.Extract(ctx, carrier)

		attrs := []attribute.KeyValue{
			semconv.MessagingSystem("kafka"),
			semconv.MessagingDestinationKindTopic,
			semconv.MessagingDestinationName(message.Topic),
			semconv.MessagingOperationReceive,
			semconv.MessagingMessageID(strconv.FormatInt(message.Offset, 10)),
			semconv.MessagingKafkaSourcePartition(int(message.Partition)),
		}
		opts := []trace.SpanStartOption{
			trace.WithAttributes(attrs...),
			trace.WithSpanKind(trace.SpanKindConsumer),
		}
		ctx, span := tracer.Start(ctx, fmt.Sprintf("%s receive", message.Topic), opts...)
		defer span.End()

		err = c(ctx, session, message)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		return
	}
}
