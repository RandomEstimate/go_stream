package task

import (
	"context"
	"github.com/RandomEstimate/go_stream/controller"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type KafkaSinkTask struct {
	controller.BaseTask

	TaskName string

	topic       string
	brokers     []string
	batchUpdate int
	timeUpdate  int // ms

	writer *kafka.Writer
	cache  []string // 数据缓存

}

func NewKafkaSinkTask(taskName string, topic string, brokers []string, batchUpdate int, timeUpdate int) *KafkaSinkTask {
	return &KafkaSinkTask{
		TaskName: taskName,
		topic:    topic, brokers: brokers,
		batchUpdate: batchUpdate,
		timeUpdate:  timeUpdate,
		cache:       make([]string, 0, batchUpdate),
	}
}

func (k *KafkaSinkTask) Open(controller *controller.StatusController) {
	controller.Register("kafka_sink_"+k.TaskName, &k.cache)

	k.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  k.brokers,
		Topic:    k.topic, // 替换为你的 Kafka 主题
		Balancer: &kafka.LeastBytes{},
	})

	k.SetRegisterStatusName([]string{"kafka_sink_" + k.TaskName})

}

func (k *KafkaSinkTask) Process(message controller.Message[any], controller *controller.StatusController) {

	if message.IsInner {

		if len(k.cache) != 0 {
			// 写入
			k.writeToKafka()
		}

	} else {
		k.cache = append(k.cache, message.Data.(string))

		if len(k.cache) >= k.batchUpdate {
			// 写入
			k.writeToKafka()
		}
	}
}

func (k *KafkaSinkTask) writeToKafka() {
	messages := make([]kafka.Message, 0, k.batchUpdate)
	for i := 0; i < len(k.cache); i++ {
		messages = append(messages, kafka.Message{
			Value: []byte(k.cache[i]),
		})
	}

	if err := k.writer.WriteMessages(context.Background(), messages...); err != nil {
		log.Fatal("failed to write messages:", err)
	}

	k.cache = k.cache[:0]

}

func (k *KafkaSinkTask) SinkRun(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(k.timeUpdate))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m := controller.Message[any]{
				Ts:      0,
				IsCk:    false,
				Data:    nil,
				IsInner: true,
			}

			k.GetReceiveMessageChan() <- m

		}
	}
}
