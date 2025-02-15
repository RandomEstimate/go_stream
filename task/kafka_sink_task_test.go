package task

import (
	"github.com/RandomEstimate/go_stream/config"
	"github.com/RandomEstimate/go_stream/controller"
	"github.com/segmentio/kafka-go"
	"reflect"
	"testing"
)

func TestKafkaSinkTask(t *testing.T) {
	config.CheckpointInterval = 60 * 1000
	config.CheckpointTimeout = 60 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_kafka_sink\\"

	c := controller.NewCenterController()

	source := NewKafkaSourceTask("source", "earliest", []string{"127.0.0.1:9092"}, "test", []int{0, 1})
	source.Init(reflect.TypeOf(kafka.Message{}), reflect.TypeOf(""))

	c.RegisterTask("kafka_source", source, "", "source")

	sink := NewKafkaSinkTask("sink", "test_write", []string{"127.0.0.1:9092"}, 100, 20)
	sink.Init(reflect.TypeOf(""), reflect.TypeOf(""))

	c.RegisterTask("kafka_sink", sink, "kafka_source", "sink")

	c.Connect("kafka_source", "kafka_sink")

	c.Start()

}

func TestKafkaSinkTaskCheckpoint(t *testing.T) {
	config.CheckpointInterval = 60 * 60 * 1000
	config.CheckpointTimeout = 60 * 60 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_kafka_sink\\"
	config.StartMode = "checkpoint"
	config.CheckpointFilePath = "C:\\tmp\\checkpoint_kafka_sink\\checkpoint-4.gob"
	config.CheckpointPublicFilePath = "C:\\tmp\\checkpoint_kafka_sink\\checkpoint-4-public.gob"

	c := controller.NewCenterController()

	source := NewKafkaSourceTask("source", "earliest", []string{"127.0.0.1:9092"}, "test", []int{0, 1})
	source.Init(reflect.TypeOf(kafka.Message{}), reflect.TypeOf(""))

	c.RegisterTask("kafka_source", source, "", "source")

	sink := NewKafkaSinkTask("sink", "test_write", []string{"127.0.0.1:9092"}, 100, 20)
	sink.Init(reflect.TypeOf(""), reflect.TypeOf(""))

	c.RegisterTask("kafka_sink", sink, "kafka_source", "sink")

	c.Connect("kafka_source", "kafka_sink")

	c.Start()

}
