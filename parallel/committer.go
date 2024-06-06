package parallel

import (
	"container/list"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/airowner/kafka"
)

type ConsumerMessageOffset struct {
	offset   int64
	done     bool
	metadata string
}

type Committer struct {
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim
	handler kafka.ConsumerHandler
	config  Config

	lock    sync.Mutex
	offsets *list.List

	doneChan    chan *ConsumerMessageOffset
	done        chan struct{}
	messageChan chan *sarama.ConsumerMessage
}

func newCommitter(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim, handler kafka.ConsumerHandler, config Config) *Committer {
	c := &Committer{
		session: session,
		claim:   claim,
		handler: handler,
		config:  config,

		offsets:     list.New(),
		doneChan:    make(chan *ConsumerMessageOffset, config.Max),
		done:        make(chan struct{}),
		messageChan: make(chan *sarama.ConsumerMessage),
	}

	go c.startParallelConsumer()
	go c.updateOffsetBlock()
	go c.autoMarkMessage()

	return c
}

func (c *Committer) startParallelConsumer() {
	wg := &sync.WaitGroup{}
	wg.Add(c.config.Max)
	session := NewSession(c.session, c.doneChan)
	for i := 0; i < c.config.Max; i++ {
		go c.startConsumer(session, wg)
	}
	wg.Wait()

	// 所有消费者都已经停止，不会在写入doneChan，这里可以安全的关闭
	close(c.doneChan)
}

func (c *Committer) startConsumer(session *Session, wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range c.messageChan {
		err := c.handler(session.Context(), session, msg)
		if err != nil {
			sarama.Logger.Printf("[ConsumerGroupHandler] error: %s, topic %s, partition %d, offset %d, message %v",
				err.Error(), msg.Topic, msg.Partition, msg.Offset, msg.Value)
		}
	}
}

// 必须在runner全部结束后保证pipeline没有写入才能进行关闭
func (c *Committer) close() {
	close(c.messageChan)

	// 等待消费完成
	<-c.done

	// 最后一次提交
	c.markOffset()
	c.session.Commit()
	c.offsets = nil
	c.session = nil
}

// push 拉取到消息后先放入待提交监控中, 因为是同步顺序放入，所以直接写入队列尾部
func (c *Committer) push(message *sarama.ConsumerMessage) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// MarkMessage 实现中会对offset进行+1提交，标记当前消息已完成，入队列前对齐数据
	c.offsets.PushBack(&ConsumerMessageOffset{offset: message.Offset + 1})
	c.messageChan <- message
}

func (c *Committer) updateOffsetBlock() {
	for block := range c.doneChan {
		c.updateOffsetQueue(block)
	}
	close(c.done)
}

func (c *Committer) updateOffsetQueue(block *ConsumerMessageOffset) {
	c.lock.Lock()
	defer c.lock.Unlock()

	node := c.offsets.Front()

	for {
		// 空链表或链表尾部
		if node == nil {
			sarama.Logger.Printf("commit了一个不存在的offset[1] topic[%s] partition[%d] offset[%d]",
				c.claim.Topic(), c.claim.Partition(), block.offset)
			return
		}

		nodeValue := getValue(node)
		if nodeValue.offset == block.offset {
			nodeValue.metadata = block.metadata
			nodeValue.done = true
			return
		}

		// 找到比自己还大的节点，不需要继续遍历了
		if block.offset < nodeValue.offset {
			sarama.Logger.Printf("commit了一个不存在的offset[2] topic[%s] partition[%d] offset[%d]",
				c.claim.Topic(), c.claim.Partition(), block.offset)
			return
		}

		node = node.Next()
	}
}

func getValue(element *list.Element) *ConsumerMessageOffset {
	if element == nil {
		return nil
	}
	return element.Value.(*ConsumerMessageOffset)
}

func (c *Committer) autoMarkMessage() {
	ticker := time.NewTicker(c.config.CommitPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.session.Context().Done(): // session结束，自动提交结束
			return
		case <-ticker.C:
			c.markOffset()
		}
	}
}

func (c *Committer) markOffset() {
	block := c.getCommitBlock()
	if block == nil {
		return
	}

	c.session.MarkOffset(c.claim.Topic(), c.claim.Partition(), block.offset, block.metadata)
}

// getCommitBlock 在所有数据中找出最小的未完成offset的前一个已完成
func (c *Committer) getCommitBlock() (block *ConsumerMessageOffset) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var pre *list.Element
	node := c.offsets.Front()

	for {
		if node == nil {
			return block
		}
		current := getValue(node)
		if !current.done {
			// 优化链表
			c.optimizationList()
			return block
		} else {
			pre = node
			node = node.Next()
			block = c.offsets.Remove(pre).(*ConsumerMessageOffset)
		}
	}
}

func (c *Committer) optimizationList() {
	l := c.offsets
	if l.Len() < c.config.Optimization {
		return
	}

	// 从后往前优化，每个已完成节点的前面所有已完成节点都可以被移除掉
	node := l.Back()

	for {
		if node == nil {
			return
		}

		if getValue(node).done {
			for {
				prev := node.Prev()
				if prev == nil {
					return
				}
				if getValue(prev).done {
					l.Remove(prev)
				} else {
					node = prev // 前一个节点未完成需要保留
					break
				}
			}
		}

		node = node.Prev()
	}
}
