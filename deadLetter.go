package redis_mq

import (
	"context"

	"github.com/bing-bing-student/redis-mq/log"
	"github.com/bing-bing-student/redis-mq/redis"
)

// DeadLetterMailbox 死信队列，当消息处理失败达到指定次数时，会被投递到此处
type DeadLetterMailbox interface {
	Deliver(ctx context.Context, msg *redis.MsgEntity) error
}

// DeadLetterLogger 默认使用的死信队列，仅仅对消息失败的信息进行日志打印
type DeadLetterLogger struct{}

func NewDeadLetterLogger() *DeadLetterLogger {
	return &DeadLetterLogger{}
}

func (d *DeadLetterLogger) Deliver(ctx context.Context, msg *redis.MsgEntity) error {
	log.ErrorContextFormat(ctx, "msg fail execeed retry limit, msg id: %s", msg.MsgID)
	return nil
}
