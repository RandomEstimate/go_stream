package task

import (
	"context"
	"fmt"
	"github.com/RandomEstimate/go_stream/controller"
	"github.com/RandomEstimate/go_stream/logger"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"time"
)

type KafkaSourceTask struct {
	controller.BaseTask

	TaskName  string
	mode      string // 两种初始化模式 earliest latest
	brokers   []string
	topic     string
	partition []int
	reader    *kafka.Reader

	offset       map[int]int64 // partition--offset
	readerConfig []kafka.ReaderConfig
}

func NewKafkaSourceTask(taskName string, mode string, brokers []string, topic string, partition []int) *KafkaSourceTask {
	return &KafkaSourceTask{TaskName: taskName,
		mode:         mode,
		brokers:      brokers,
		topic:        topic,
		partition:    partition,
		offset:       make(map[int]int64),
		readerConfig: make([]kafka.ReaderConfig, 0),
	}
}

func (k *KafkaSourceTask) Open(controller *controller.StatusController) {
	controller.Register("kafka_source_"+k.TaskName, &k.offset)

	var startOffset int64
	if k.mode == "earliest" {
		startOffset = kafka.FirstOffset
	} else if k.mode == "latest" {
		startOffset = kafka.LastOffset
	}
	// 创建reader config
	for _, p := range k.partition {
		k.readerConfig = append(k.readerConfig, kafka.ReaderConfig{
			Brokers:     k.brokers,
			Topic:       k.topic,
			StartOffset: startOffset,
			MaxAttempts: 10,
			MaxWait:     time.Second * 60,
			Partition:   p,
			Logger: kafka.LoggerFunc(func(s string, i ...interface{}) {
				logger.Log.WithFields(logrus.Fields{
					"KafkaSource": k.TaskName,
				}).Info(fmt.Sprintf(s, i))
			}), // 可选日志
			ErrorLogger: kafka.LoggerFunc(func(s string, i ...interface{}) {
				logger.Log.WithFields(logrus.Fields{
					"KafkaSourceError": k.TaskName,
				}).Info(fmt.Sprintf(s, i))
			}), // 错误日志
		})
	}

	k.SetRegisterStatusName([]string{"kafka_source_" + k.TaskName})

}

func (k *KafkaSourceTask) Run(ctx context.Context) {
	// 启动开启消费
	for i := 0; i < len(k.readerConfig); i++ {
		go func(i int) {
			reader := kafka.NewReader(k.readerConfig[i])
			defer reader.Close()

			if offset, ok := k.offset[k.partition[i]]; ok {
				err := reader.SetOffset(offset)
				if err != nil {
					panic(fmt.Sprintf("设置kafka source offset 错误 %v", err))
				}
			}

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// 读取消息（不自动提交）
					m, err := reader.ReadMessage(context.Background())
					if err != nil {
						panic(err)
					}

					message := controller.Message[any]{
						Ts:   0, // 目前还没有对Ts进行处理
						IsCk: false,
						Data: m,
					}

					k.GetReceiveMessageChan() <- message
				}
			}
		}(i)

	}

}

func (k *KafkaSourceTask) Process(message controller.Message[any], controller *controller.StatusController) {
	m := message.Data.(kafka.Message)
	message.Data = string(message.Data.(kafka.Message).Value)
	message.Key = "kafka_source"

	k.offset[m.Partition] = m.Offset + 1
	k.SendMessage(message)

}
