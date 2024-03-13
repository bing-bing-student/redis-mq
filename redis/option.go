package redis

const (
	// DefaultIdleTimeoutSeconds 默认连接池超过 10s 释放连接
	DefaultIdleTimeoutSeconds = 10
	// DefaultMaxActive 默认最大激活连接数
	DefaultMaxActive = 100
	// DefaultMaxIdle 默认最大空闲连接数
	DefaultMaxIdle = 20
)

// ClientOptions 中的network,address,password为必填
type ClientOptions struct {
	maxIdle            int
	idleTimeoutSeconds int
	maxActive          int
	wait               bool
	network            string
	address            string
	password           string
}

type ClientOption func(c *ClientOptions)

// WithMaxIdle 设置最大空闲连接数
func WithMaxIdle(maxIdle int) ClientOption {
	return func(c *ClientOptions) {
		c.maxIdle = maxIdle
	}
}

// WithIdleTimeoutSeconds 设置连接池释放连接的超时连接
func WithIdleTimeoutSeconds(idleTimeoutSeconds int) ClientOption {
	return func(c *ClientOptions) {
		c.idleTimeoutSeconds = idleTimeoutSeconds
	}
}

// WithMaxActive 设置最大激活连接数
func WithMaxActive(maxActive int) ClientOption {
	return func(c *ClientOptions) {
		c.maxActive = maxActive
	}
}

// WithWaitMode 设置是否阻塞
func WithWaitMode() ClientOption {
	return func(c *ClientOptions) {
		c.wait = true
	}
}

func repairClient(c *ClientOptions) {
	if c.maxIdle < 0 {
		c.maxIdle = DefaultMaxIdle
	}

	if c.idleTimeoutSeconds < 0 {
		c.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}

	if c.maxActive < 0 {
		c.maxActive = DefaultMaxActive
	}
}
