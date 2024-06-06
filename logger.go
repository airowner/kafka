package kafka

import "github.com/IBM/sarama"

func SetLogger(logger sarama.StdLogger) {
	sarama.Logger = logger
}

type NopLogger struct{}

func (n NopLogger) Print(v ...interface{})                 {}
func (n NopLogger) Printf(format string, v ...interface{}) {}
func (n NopLogger) Println(v ...interface{})               {}
