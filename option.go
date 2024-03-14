package redis_mq

import "time"

type ProducerOptions struct {
	msgQueueLen int
}

type ProducerOption func(opts *ProducerOptions)

func WithMsgQueueLen(len int) ProducerOption {
	return func(opts *ProducerOptions) {
		opts.msgQueueLen = len
	}
}

func repairProducer(opts *ProducerOptions) {
	if opts.msgQueueLen <= 0 {
		opts.msgQueueLen = 500
	}
}

type ConsumerOptions struct {
	// 每轮接收消息的超时时长
	receiveTimeout time.Duration
	// 处理消息的最大重试次数，超过此次数时，消息会被投递到死信队列
	maxRetryLimit int
	// 死信队列，可以由使用方自定义实现
	deadLetterMailbox DeadLetterMailbox
	// 投递死信流程超时阈值
	deadLetterDeliverTimeout time.Duration
	// 处理消息流程超时阈值
	handleMsgTimeout time.Duration
}

type ConsumerOption func(opts *ConsumerOptions)

func WithReceiveTimeout(timeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.receiveTimeout = timeout
	}
}

func WithMaxRetryLimit(maxRetryLimit int) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.maxRetryLimit = maxRetryLimit
	}
}

func WithDeadLetterMailbox(mailbox DeadLetterMailbox) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadLetterMailbox = mailbox
	}
}

func WithDeadLetterDeliverTimeout(timeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadLetterDeliverTimeout = timeout
	}
}

func WithHandleMsgTimeout(timeout time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.handleMsgTimeout = timeout
	}
}

func repairConsumer(opts *ConsumerOptions) {
	if opts.receiveTimeout < 0 {
		opts.receiveTimeout = 2 * time.Second
	}

	if opts.maxRetryLimit < 0 {
		opts.maxRetryLimit = 3
	}

	if opts.deadLetterMailbox == nil {
		opts.deadLetterMailbox = NewDeadLetterLogger()
	}

	if opts.deadLetterDeliverTimeout <= 0 {
		opts.deadLetterDeliverTimeout = time.Second
	}

	if opts.handleMsgTimeout <= 0 {
		opts.handleMsgTimeout = time.Second
	}
}
