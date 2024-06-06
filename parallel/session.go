package parallel

import "github.com/IBM/sarama"

type Session struct {
	sarama.ConsumerGroupSession
	doneChan chan<- *ConsumerMessageOffset
}

func NewSession(session sarama.ConsumerGroupSession, doneChan chan<- *ConsumerMessageOffset) *Session {
	return &Session{
		ConsumerGroupSession: session,
		doneChan:             doneChan,
	}
}

func (m *Session) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, metadata)
}

func (m *Session) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	m.doneChan <- &ConsumerMessageOffset{
		offset:   offset,
		done:     true,
		metadata: metadata,
	}
}
