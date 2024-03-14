## 前言

使用此 sdk 进行实践前，建议先行了解与 redis streams 有关的特性。[redis streams](https://redis.io/docs/data-types/streams/)

## 基本使用

### 生产者

```go
import (
	"context"
	"fmt"
	pubsub "github.com/bing-bing-student/redis-mq"
	"github.com/bing-bing-student/redis-mq/redis"
)

const (
	network  = "tcp"
	address  = "127.0.0.1:6379"
	password = ""
	topic    = "test_topic"
)

func main() {
	client := redis.NewClient(network, address, password)
	// 最多保留二十条消息
	producer := pubsub.NewProducer(client, pubsub.WithMsgQueueLen(20))
	ctx := context.Background()
	msgID, err := producer.SendMsg(ctx, topic, "test_key", "test_value")
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(msgID)
}
```

运行结果：

```
# go run main.go
1710403869444-0
```

### 消费者

```
import (
	"context"
	"fmt"
	pubsub "github.com/bing-bing-student/redis-mq"
	"github.com/bing-bing-student/redis-mq/redis"
	"time"
)

const (
	network       = "tcp"
	address       = "127.0.0.1:6379"
	password      = ""
	topic         = "test_topic"
	consumerGroup = "test_group"
	consumerID    = "test_consumer"
)

// DemoDeadLetterMailbox 自定义实现的死信队列
type DemoDeadLetterMailbox struct {
	do func(msg *redis.MsgEntity)
}

func NewDemoDeadLetterMailbox(do func(msg *redis.MsgEntity)) *DemoDeadLetterMailbox {
	return &DemoDeadLetterMailbox{
		do: do,
	}
}

// Deliver 死信队列接收消息的处理方法
func (d *DemoDeadLetterMailbox) Deliver(ctx context.Context, msg *redis.MsgEntity) error {
	d.do(msg)
	return nil
}

func main() {
	client := redis.NewClient(network, address, password)

	// 接收到消息后的处理函数
	callbackFunc := func(ctx context.Context, msg *redis.MsgEntity) error {
		fmt.Printf("receive msg, msg id: %s, msg key: %s, msg val: %s\n", msg.MsgID, msg.Key, msg.Val)
		return nil
	}

	// 自定义实现的死信队列
	demoDeadLetterMailbox := NewDemoDeadLetterMailbox(func(msg *redis.MsgEntity) {
		fmt.Printf("receive dead letter, msg id: %s, msg key: %s, msg val: %s\n", msg.MsgID, msg.Key, msg.Val)
	})

	consumer, _ := pubsub.NewConsumer(client, topic, consumerGroup, consumerID, callbackFunc,
		// 每条消息最多重试 2 次
		pubsub.WithMaxRetryLimit(2),
		// 每轮接收消息的超时时间为 2 s
		pubsub.WithReceiveTimeout(2*time.Second),
		// 注入自定义实现的死信队列
		pubsub.WithDeadLetterMailbox(demoDeadLetterMailbox))
	defer consumer.Stop()
	<-time.After(5 * time.Second)
}
```

运行结果：

```
# go run main.go
receive msg, msg id: 1710403869444-0, msg key: test_key, msg val: test_value
```



