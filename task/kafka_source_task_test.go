package task

import (
	"github.com/RandomEstimate/go_stream/config"
	"github.com/RandomEstimate/go_stream/controller"
	"github.com/segmentio/kafka-go"
	"reflect"
	"testing"
)

func TestKafkaSourceTask(t *testing.T) {
	config.CheckpointInterval = 5 * 1000
	config.CheckpointTimeout = 5 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_kafka_source\\"

	c := controller.NewCenterController()

	source := NewKafkaSourceTask("source", "earliest", []string{"127.0.0.1:9092"}, "test", []int{0, 1})
	source.Init(reflect.TypeOf(kafka.Message{}), reflect.TypeOf(""))

	c.RegisterTask("kafka_source", source, "", "source")

	node1 := PrintTask{}
	node1.Init(reflect.TypeOf(""), reflect.TypeOf(""))

	c.RegisterTask("node1", &node1, "kafka_source", "common")

	c.Connect("kafka_source", "node1")

	c.Start()

}

func TestKafkaSourceTask2(t *testing.T) {
	config.CheckpointInterval = 6 * 60 * 1000
	config.CheckpointTimeout = 6 * 60 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_kafka_source\\"

	config.StartMode = "checkpoint"
	config.CheckpointFilePath = "C:\\tmp\\checkpoint_kafka_source\\checkpoint-3.gob"
	config.CheckpointPublicFilePath = "C:\\tmp\\checkpoint_kafka_source\\checkpoint-3-public.gob"

	c := controller.NewCenterController()

	source := NewKafkaSourceTask("source", "earliest", []string{"127.0.0.1:9092"}, "test", []int{0, 1})
	source.Init(reflect.TypeOf(kafka.Message{}), reflect.TypeOf(""))

	c.RegisterTask("kafka_source", source, "", "source")

	node1 := &PrintTask{}
	node1.Init(reflect.TypeOf(""), reflect.TypeOf(""))
	c.RegisterTask("node1", node1, "kafka_source", "common")
	c.Connect("kafka_source", "node1")

	c.Start()

}
