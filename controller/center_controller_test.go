package controller

import (
	"context"
	"encoding/gob"
	"fmt"
	"go_stream/config"
	"reflect"
	"testing"
	"time"
)

type NumberTask struct {
	BaseTask

	taskName string

	sum     int
	flag    bool
	intList []int
}

func (a *NumberTask) Open(controller *StatusController) {
	fmt.Println("task", a.taskName, "Open")
	// 初始化变量以及注册
	controller.Register(a.taskName+"_sum", &a.sum)
	controller.Register(a.taskName+"_flag", &a.flag)
	controller.Register(a.taskName+"_intList", &a.intList)
	gob.Register(make([]int, 0))

	a.SetRegisterStatusName([]string{a.taskName + "_sum", a.taskName + "_flag", a.taskName + "_intList"})

	a.sum = 0
	a.flag = false
	a.intList = make([]int, 0)
}

func (a *NumberTask) Process(message Message[any], controller *StatusController) {
	// 模拟初始化
	if a.flag == false {
		a.sum = -100
		a.flag = true
	}

	a.sum += message.Data.(int)
	a.intList = append(a.intList, message.Data.(int))
	// 如果长度超过3则只保留三位
	if len(a.intList) > 3 {
		a.intList = a.intList[len(a.intList)-3:]
	}

	// 配置后续key
	message.Key = "number"

	a.SendMessage(message)

	fmt.Println("taskName 【", a.taskName, "】 deal number is ", message.Data.(int), "sum is ", a.sum, "iniList is ", a.intList)

}

type NumberSource struct {
	BaseTask

	taskName string

	dealNumber int
}

func (a *NumberSource) Open(controller *StatusController) {
	fmt.Println("数据源Open")
	controller.Register(a.taskName+"_dealNumber", &a.dealNumber)
	a.SetRegisterStatusName([]string{a.taskName + "_dealNumber"})
	a.dealNumber = 0
}

func (a *NumberSource) Process(message Message[any], controller *StatusController) {
	a.dealNumber = message.Data.(int) + 1 // 记录下一次开始的位置 类似kafka 记录下一次开始的offset
	a.SendMessage(message)
}

func (a *NumberSource) Run(ctx context.Context) {
	fmt.Println("Run 被调用")
	count := a.dealNumber
	timer := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			key := "even"
			if a.dealNumber%2 != 0 {
				key = "odd"
			}
			m := Message[any]{
				Ts:   0,
				IsCk: false,
				Data: count,
				Key:  key,
			}
			fmt.Println("Run 发送消息", count)
			a.GetReceiveMessageChan() <- m
			count += 1
		}
	}
}

func TestCenterController(t *testing.T) {
	/*
		测试思路

		首先需要定义一个Source task
		一个消费task
		生产者负责每秒生产一个数字
		消费者有两个一个定义偶数相加一个定义奇数相加

		通过初始化注册状态变量
		保存中间变量以及ck

		最后测试中途卸载并添加task
	*/

	// 修改基础配置xx
	/*
		// checkpoint 配置

		var CheckpointInterval int64 = 60 * 60 * 100
		var CheckpointNumber int = 1
		var CheckpointTimeout int64 = 30 * 60 * 100
		var CheckpointPath string = ""

		// 启动状态配置

		var StartMode = "standard"        // standard | checkpoint
		var CheckpointFilePath = ""       // checkpoint 模式下文件路径
		var CheckpointPublicFilePath = "" // checkpoint 模式下文件路径
	*/

	config.CheckpointInterval = 5 * 1000
	config.CheckpointTimeout = 5 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_center_test\\"

	controller := NewCenterController()

	evenTask := NumberTask{
		taskName: "even",
	}
	evenTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	oddTask := NumberTask{
		taskName: "odd",
	}
	oddTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	numberTask := NumberTask{
		taskName: "number",
	}
	numberTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	sourceTask := NumberSource{
		taskName: "source",
	}
	sourceTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	controller.RegisterTask("even", &evenTask, "even", "common")
	controller.RegisterTask("odd", &oddTask, "odd", "common")
	controller.RegisterTask("source", &sourceTask, "", "source")
	controller.RegisterTask("number", &numberTask, "number", "common")

	// 定义一下连接情况
	controller.Connect("source", "even")
	controller.Connect("source", "odd")
	controller.Connect("even", "number")
	controller.Connect("odd", "number")

	go func() {
		ticker := time.NewTicker(time.Second * 60)
		for {
			select {
			case <-ticker.C:
				statistic, err := controller.GetTaskStatistic("odd")
				if err != nil {
					panic(err)
				}
				fmt.Println("get statistic", statistic)
			}
		}
	}()

	controller.GetGraph()

	controller.Start()

}

func TestCenterControllerCheckPointMode(t *testing.T) {
	config.CheckpointInterval = 30 * 1000
	config.CheckpointTimeout = 5 * 1000
	config.CheckpointPath = "C:\\tmp\\checkpoint_center_test\\"

	config.StartMode = "checkpoint"
	config.CheckpointFilePath = "C:\\tmp\\checkpoint_center_test\\checkpoint-3.gob"
	config.CheckpointPublicFilePath = "C:\\tmp\\checkpoint_center_test\\checkpoint-3-public.gob"

	controller := NewCenterController()

	evenTask := NumberTask{
		taskName: "even",
	}
	evenTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	oddTask := NumberTask{
		taskName: "odd",
	}
	oddTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	numberTask := NumberTask{
		taskName: "number",
	}
	numberTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	sourceTask := NumberSource{
		taskName: "source",
	}
	sourceTask.Init(reflect.TypeOf(int(1)), reflect.TypeOf(int(1)))

	controller.RegisterTask("even", &evenTask, "even", "common")
	controller.RegisterTask("odd", &oddTask, "odd", "common")
	controller.RegisterTask("source", &sourceTask, "", "source")
	controller.RegisterTask("number", &numberTask, "number", "common")

	// 定义一下连接情况
	controller.Connect("source", "even")
	controller.Connect("source", "odd")
	controller.Connect("even", "number")
	controller.Connect("odd", "number")

	controller.Start()

}
