package task

import (
	"fmt"
	"github.com/RandomEstimate/go_stream/config"
	"github.com/RandomEstimate/go_stream/controller"
	"github.com/segmentio/kafka-go"
	"reflect"
	"testing"
)

func TestMysqlSinkTask(t *testing.T) {

	config.CheckpointInterval = 60 * 1000
	config.CheckpointTimeout = 60 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_mysql_sink\\"

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

	sink := NewMysqlSinkTask("sink", "127.0.0.1", 3306, "root", "751037790qQ!", "test", 100, 20, "insert into %s(col1, col2) values ", func(data any) (string, string) {
		s := data.(string)
		return "test", fmt.Sprintf("('%s', %s)", s, s)
	}, func(data any) (string, string) {
		tableSql := `
		create table if not exists test(
			col1 varchar(10),
			col2 int
		)
		`
		return "test", tableSql

	})
	sink.Init(reflect.TypeOf(""), reflect.TypeOf(""))
	c.RegisterTask("mysql_sink", sink, "kafka_source", "sink")

	c.Connect("kafka_source", "mysql_sink")

	c.Start()

}
