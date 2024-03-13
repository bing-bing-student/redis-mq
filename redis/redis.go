package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/demdxx/gocast"
	"github.com/gomodule/redigo/redis"
)

type MsgEntity struct {
	MsgID string
	Key   string
	Val   string
}

var ErrNoMsg = errors.New("no message received")

// Client 表示 Redis 客户端
type Client struct {
	options *ClientOptions
	pool    *redis.Pool
}

// NewClient 新建客户端, 适用于简单或标准的Redis连接需求
func NewClient(network, address, password string, opts ...ClientOption) *Client {
	c := Client{
		options: &ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}

	for _, opt := range opts {
		opt(c.options)
	}
	repairClient(c.options)

	pool := c.getRedisPool()
	return &Client{
		pool: pool,
	}
}

// NewClientWithPool 新建客户端, 适用于需要自定义连接池配置的场景
func NewClientWithPool(pool *redis.Pool, opts ...ClientOption) *Client {
	c := Client{
		pool:    pool,
		options: &ClientOptions{},
	}

	for _, opt := range opts {
		opt(c.options)
	}
	repairClient(c.options)

	return &c
}

// 返回 redis 连接池的配置信息
func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     c.options.maxIdle,
		IdleTimeout: time.Duration(c.options.idleTimeoutSeconds) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		MaxActive: c.options.maxActive,
		Wait:      c.options.wait,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// GetConn 得到连接上下文
func (c *Client) GetConn(ctx context.Context) (redis.Conn, error) {
	return c.pool.GetContext(ctx)
}

// getRedisConn 得到 redis 连接
func (c *Client) getRedisConn() (redis.Conn, error) {
	if c.options.address == "" {
		panic("Cannot get redis address from config")
	}

	var dialOpts []redis.DialOption
	if len(c.options.password) > 0 {
		// 注入密码
		dialOpts = append(dialOpts, redis.DialPassword(c.options.password))
	}

	// 创建新的连接
	conn, err := redis.DialContext(context.Background(), c.options.network, c.options.address, dialOpts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// XAddMsg 生产者将消息放入MQ
// 需要注意的是: 消息的ID在当前接口下只能使用redis数据库自动生成的ID,不能自定义消息ID
func (c *Client) XAddMsg(ctx context.Context, topic string, maxLen int, key, val string) (string, error) {
	if topic == "" {
		return "", errors.New("redis XADD topic can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}

	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	return redis.String(conn.Do("XADD", topic, "MAXLEN", maxLen, "*", key, val))
}

// XGroupCreate 创建消费者组
func (c *Client) XGroupCreate(ctx context.Context, topic, group string) (string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	return redis.String(conn.Do("XGROUP", "CREATE", topic, group, "0-0"))
}

// XAck 消息确认机制
func (c *Client) XAck(ctx context.Context, topic, groupID, msgID string) error {
	if topic == "" || groupID == "" || msgID == "" {
		return errors.New("redis XAck topic | group_id | msg_ id can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	reply, err := redis.Int64(conn.Do("XACK", topic, groupID, msgID))
	if err != nil {
		return err
	}
	if reply != 1 {
		return fmt.Errorf("invalid reply: %d", reply)
	}

	return nil
}

// XReadGroupOldMsg 从Redis的Stream中读取那些已被消费组认领但还未被确认的旧消息(即处于"pending"状态的消息)
func (c *Client) XReadGroupOldMsg(ctx context.Context, groupID, consumerID, topic string) ([]*MsgEntity, error) {
	// pending为true表示消费旧消息, 为false表示消费新消息
	return c.xReadGroup(ctx, groupID, consumerID, topic, 0, true)
}

// XReadGroupNewMsg 表示消费新消息, 如果新消息没有到来就会阻塞, 阻塞时间为timeoutMilliseconds
func (c *Client) XReadGroupNewMsg(ctx context.Context, groupID, consumerID, topic string, timeoutMilliseconds int) ([]*MsgEntity, error) {
	return c.xReadGroup(ctx, groupID, consumerID, topic, timeoutMilliseconds, false)
}

func (c *Client) xReadGroup(ctx context.Context, groupID, consumerID, topic string, timeoutMilliseconds int, pending bool) ([]*MsgEntity, error) {
	// 参数校验
	if groupID == "" || consumerID == "" || topic == "" {
		return nil, errors.New("redis XREADGROUP groupID/consumerID/topic can't be empty")
	}

	// 得到连接上下文
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	var rawReply interface{}
	if pending {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "STREAMS", topic, "0-0")
	} else {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "BLOCK", timeoutMilliseconds, "STREAMS", topic, ">")
	}

	// 异常处理
	if err != nil {
		return nil, err
	}
	reply, _ := rawReply.([]interface{})
	if len(reply) == 0 {
		return nil, ErrNoMsg
	}
	replyElement, _ := reply[0].([]interface{})
	if len(replyElement) != 2 {
		return nil, errors.New("invalid msg format")
	}

	// 对消费到的数据进行格式化
	var msg []*MsgEntity
	rawMsg, _ := replyElement[1].([]interface{})
	for _, rawMsg := range rawMsg {
		_msg, _ := rawMsg.([]interface{})
		if len(_msg) != 2 {
			return nil, errors.New("invalid msg format")
		}
		msgID := gocast.ToString(_msg[0])
		msgBody, _ := _msg[1].([]interface{})
		if len(msgBody) != 2 {
			return nil, errors.New("invalid msg format")
		}
		msgKey := gocast.ToString(msgBody[0])
		msgVal := gocast.ToString(msgBody[1])
		msg = append(msg, &MsgEntity{
			MsgID: msgID,
			Key:   msgKey,
			Val:   msgVal,
		})
	}

	return msg, nil
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", errors.New("redis GET key can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	return redis.String(conn.Do("GET", key))
}

func (c *Client) Set(ctx context.Context, key, value string) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET key or value can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	resp, err := conn.Do("SET", key, value)
	if err != nil {
		return -1, err
	}

	if respStr, ok := resp.(string); ok && strings.ToLower(respStr) == "ok" {
		return 1, nil
	}

	return redis.Int64(resp, err)
}

func (c *Client) SetNEX(ctx context.Context, key, value string, expireSeconds int64) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET keyNX or value can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	reply, err := conn.Do("SET", key, value, "EX", expireSeconds, "NX")
	if err != nil {
		return -1, err
	}

	if respStr, ok := reply.(string); ok && strings.ToLower(respStr) == "ok" {
		return 1, nil
	}

	return redis.Int64(reply, err)
}

func (c *Client) SetNX(ctx context.Context, key, value string) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET key NX or value can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	reply, err := conn.Do("SET", key, value, "NX")
	if err != nil {
		return -1, err
	}

	if respStr, ok := reply.(string); ok && strings.ToLower(respStr) == "ok" {
		return 1, nil
	}

	return redis.Int64(reply, err)
}

func (c *Client) Del(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("redis DEL key can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	_, err = conn.Do("DEL", key)
	return err
}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return -1, errors.New("redis INCR key can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	return redis.Int64(conn.Do("INCR", key))
}

// Eval 支持使用 lua 脚本
func (c *Client) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer func(conn redis.Conn) {
		_ = conn.Close()
	}(conn)

	return conn.Do("EVAL", args...)
}
