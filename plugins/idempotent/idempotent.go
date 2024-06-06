package idempotent

import (
	"fmt"

	"github.com/IBM/sarama"
)

type Idempotent interface {
	Done(message *sarama.ConsumerMessage) bool     // 返回true，表示此消息已经被消费过
	SetDone(message *sarama.ConsumerMessage) error // 可能被多次调用
}

type NopIdempotent struct{}

func (d *NopIdempotent) Done(message *sarama.ConsumerMessage) bool {
	return false
}

func (d *NopIdempotent) SetDone(message *sarama.ConsumerMessage) error {
	return nil
}

type IdempotentStore interface {
	Exist(key string) bool
	Store(key string) error
}

type Idempotentor struct {
	storage IdempotentStore
}

func NewIdempotentor(storage IdempotentStore) *Idempotentor {
	return &Idempotentor{
		storage: storage,
	}
}

func (d *Idempotentor) Done(message *sarama.ConsumerMessage) bool {
	key := fmt.Sprintf("%s|%d|%d", message.Topic, message.Partition, message.Offset)
	return d.storage.Exist(key)
}

func (d *Idempotentor) SetDone(message *sarama.ConsumerMessage) error {
	key := fmt.Sprintf("%s|%d|%d", message.Topic, message.Partition, message.Offset)
	return d.storage.Store(key)
}
