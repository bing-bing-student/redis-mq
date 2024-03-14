package redis_mq

import (
	"context"
	"errors"

	"github.com/bing-bing-student/redis-mq/log"
	"github.com/bing-bing-student/redis-mq/redis"
)

// MsgCallback 接收到消息后执行的回调函数
type MsgCallback func(ctx context.Context, msg *redis.MsgEntity) error

// Consumer 消费者
type Consumer struct {
	// consumer 生命周期管理
	ctx  context.Context
	stop context.CancelFunc

	// 接收到 msg 时执行的回调函数，由使用方定义
	callbackFunc MsgCallback

	// redis 客户端，基于 redis 实现 message queue
	client *redis.Client

	// 消费的 topic
	topic string
	// 所属的消费者组
	groupID string
	// 当前节点的消费者 id
	consumerID string

	// 各消息累计失败次数
	failureCounts map[redis.MsgEntity]int

	// 一些用户自定义的配置
	opts *ConsumerOptions
}

func NewConsumer(client *redis.Client, topic, groupID, consumerID string, callbackFunc MsgCallback, opts ...ConsumerOption) (*Consumer, error) {

	ctx, stop := context.WithCancel(context.Background())
	c := Consumer{
		client:       client,
		ctx:          ctx,
		stop:         stop,
		callbackFunc: callbackFunc,
		topic:        topic,
		groupID:      groupID,
		consumerID:   consumerID,

		opts: &ConsumerOptions{},

		failureCounts: make(map[redis.MsgEntity]int),
	}

	if err := c.checkParam(); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(c.opts)
	}

	repairConsumer(c.opts)

	go c.run()
	return &c, nil
}

func (c *Consumer) checkParam() error {
	if c.callbackFunc == nil {
		return errors.New("callback function can't be empty")
	}

	if c.client == nil {
		return errors.New("redis client can't be empty")
	}

	if c.topic == "" || c.consumerID == "" || c.groupID == "" {
		return errors.New("topic | group_id | consumer_id can't be empty")
	}

	return nil
}

// Stop 停止 consumer
func (c *Consumer) Stop() {
	c.stop()
}

// 运行消费者
func (c *Consumer) run() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// 新消息接收处理
		msg, err := c.receive()
		if err != nil {
			log.ErrorContextFormat(c.ctx, "receive msg failed, err: %v", err)
			continue
		}

		ctx, _ := context.WithTimeout(c.ctx, c.opts.handleMsgTimeout)
		c.handlerMsg(ctx, msg)

		// 死信队列投递
		ctx, _ = context.WithTimeout(c.ctx, c.opts.deadLetterDeliverTimeout)
		c.deliverDeadLetter(ctx)

		// pending 消息接收处理
		pendingMsg, err := c.receivePending()
		if err != nil {
			log.ErrorContextFormat(c.ctx, "pending msg received failed, err: %v", err)
			continue
		}

		ctx, _ = context.WithTimeout(c.ctx, c.opts.handleMsgTimeout)
		c.handlerMsg(ctx, pendingMsg)
	}
}

func (c *Consumer) receive() ([]*redis.MsgEntity, error) {
	msg, err := c.client.XReadGroupNewMsg(c.ctx, c.groupID, c.consumerID, c.topic, int(c.opts.receiveTimeout.Milliseconds()))
	if err != nil && !errors.Is(err, redis.ErrNoMsg) {
		return nil, err
	}

	return msg, nil
}

func (c *Consumer) receivePending() ([]*redis.MsgEntity, error) {
	pendingMsg, err := c.client.XReadGroupOldMsg(c.ctx, c.groupID, c.consumerID, c.topic)
	if err != nil && !errors.Is(err, redis.ErrNoMsg) {
		return nil, err
	}

	return pendingMsg, nil
}

func (c *Consumer) handlerMsg(ctx context.Context, messages []*redis.MsgEntity) {
	for _, msg := range messages {
		if err := c.callbackFunc(ctx, msg); err != nil {
			// 失败计数器累加
			c.failureCounts[*msg]++
			continue
		}

		// callback 执行成功，进行 ack
		if err := c.client.XAck(ctx, c.topic, c.groupID, msg.MsgID); err != nil {
			log.ErrorContextFormat(ctx, "msg ack failed, msg id: %s, err: %v", msg.MsgID, err)
			continue
		}

		delete(c.failureCounts, *msg)
	}
}

func (c *Consumer) deliverDeadLetter(ctx context.Context) {
	// 对于失败达到指定次数的消息，投递到死信中，然后执行 ack
	for msg, failureCnt := range c.failureCounts {
		if failureCnt < c.opts.maxRetryLimit {
			continue
		}

		// 投递死信队列
		if err := c.opts.deadLetterMailbox.Deliver(ctx, &msg); err != nil {
			log.ErrorContextFormat(c.ctx, "dead letter deliver failed, msg id: %s, err: %v", msg.MsgID, err)
		}

		// 执行 ack 响应
		if err := c.client.XAck(ctx, c.topic, c.groupID, msg.MsgID); err != nil {
			log.ErrorContextFormat(c.ctx, "msg ack failed, msg id: %s, err: %v", msg.MsgID, err)
			continue
		}

		// 对于 ack 成功的消息，将其从 failure map 中删除
		delete(c.failureCounts, msg)
	}
}
