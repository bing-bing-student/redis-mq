package redis_mq

import (
	"context"

	"github.com/bing-bing-student/redis-mq/redis"
)

type Producer struct {
	client *redis.Client
	opts   *ProducerOptions
}

func NewProducer(client *redis.Client, opts ...ProducerOption) *Producer {
	p := Producer{
		client: client,
		opts:   &ProducerOptions{},
	}

	for _, opt := range opts {
		opt(p.opts)
	}

	repairProducer(p.opts)

	return &p
}

// SendMsg 生产一条消息
func (p *Producer) SendMsg(ctx context.Context, topic, key, val string) (string, error) {
	return p.client.XAddMsg(ctx, topic, p.opts.msgQueueLen, key, val)
}
