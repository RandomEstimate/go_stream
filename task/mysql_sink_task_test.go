package task

import (
	"go_stream/config"
	"go_stream/controller"
	"fmt"
	"github.com/segmentio/kafka-go"
	"reflect"
	"testing"
)

func TestMysqlSinkTask(t *testing.T) {

	config.CheckpointInterval = 60 * 1000
	config.CheckpointTimeout = 60 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_kafka_sink\\"

	c := controller.NewCenterController()

	source := NewKafkaSourceTask("source", "earliest", []string{"127.0.0.1:9092"}, "test", []int{0, 1})
	source.Init(reflect.TypeOf(kafka.Message{}), reflect.TypeOf(""))

	c.RegisterTask("kafka_source", source, "", "source")

	/*
		mysql 建表语句
		create table test (
			col1 varchar(10),
			col2 int
		)
	*/

	sink := NewMysqlSinkTask("sink", "127.0.0.1", 3306, "root", "751037790qQ!", "test", 100, 20, "insert into test(col1, col2) values ", func(data any) string {
		s := data.(string)
		return fmt.Sprintf("('%s', %s)", s, s)
	})
	sink.Init(reflect.TypeOf(""), reflect.TypeOf(""))
	c.RegisterTask("mysql_sink", sink, "kafka_source", "sink")

	c.Start()

}
