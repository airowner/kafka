package parallel

import "time"

var DefaultConfig = Config{
	Max:          4,
	CommitPeriod: time.Second,
	Optimization: 100,
}

type Config struct {
	// Max 最大并发数
	Max int `yaml:"Max"`

	// CommitPeriod 默认1s
	// commit 提交周期，每隔多少时间提交一次, 队列最前面的数据未操作完成，忽略提交
	CommitPeriod time.Duration

	// Optimization 默认100
	// 由于并发操作，会导致数据完成有先后顺序，当队列前面数据长时间未完成，后面数据可能出现已完成
	// 超过此数量进行队列优化，以减少队列数据总量，提高插入速度
	Optimization int
}
