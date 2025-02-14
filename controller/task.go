package controller

import (
	"context"
	"reflect"
)

type Task interface {

	// GetReceiveType 获取当前任务接收数据类型
	GetReceiveType() reflect.Type

	// GetSendType 获取发送数据类型
	GetSendType() reflect.Type

	// GetReceiveMessageChan 获取接收数据通道
	GetReceiveMessageChan() chan Message[any]

	// GetSendMessageChan 获取发送数据通道
	GetSendMessageChan() chan Message[any]

	// SendMessage 发送数据
	SendMessage(message Message[any])

	// GetRegisterStatusName 获取注册状态名称
	GetRegisterStatusName() []string

	// GetCacheList 获取缓存列表
	GetCacheList() []Message[any]

	// ClearCacheList 清理缓存列表
	ClearCacheList()

	// AppendCacheList 添加到缓存列表
	AppendCacheList(message Message[any])

	// Open 初始化注册
	Open(controller *StatusController)

	// Process 执行逻辑
	Process(message Message[any], controller *StatusController)

	// Close 执行退出
	Close(controller *StatusController)

	// Run 数据源任务
	Run(ctx context.Context)

	// SinkRun sink发送定时任务进行提交
	SinkRun(ctx context.Context)
}

type BaseTask struct {
	Receive chan Message[any]
	Send    chan Message[any]

	CacheMessage       []Message[any] // 只会在ck时触发
	RegisterStatusName []string       // 注册状态变量名称

	ReceiveType reflect.Type
	SendType    reflect.Type
}

func (b *BaseTask) Init(receiveType reflect.Type, sendType reflect.Type) {
	b.Receive = make(chan Message[any], 1000)
	b.Send = make(chan Message[any], 1000)

	b.CacheMessage = make([]Message[any], 0)

	b.ReceiveType = receiveType
	b.SendType = sendType

}

func (b *BaseTask) SetRegisterStatusName(name []string) {
	b.RegisterStatusName = name
}

func (b *BaseTask) GetReceiveType() reflect.Type {
	return b.ReceiveType
}

func (b *BaseTask) GetSendType() reflect.Type {
	return b.SendType
}

func (b *BaseTask) GetReceiveMessageChan() chan Message[any] {
	return b.Receive
}

func (b *BaseTask) GetSendMessageChan() chan Message[any] {
	return b.Send
}

func (b *BaseTask) SendMessage(message Message[any]) {
	b.Send <- message
}

func (b *BaseTask) GetRegisterStatusName() []string {
	return b.RegisterStatusName
}

func (b *BaseTask) GetCacheList() []Message[any] {
	cacheList := make([]Message[any], 0)
	for _, v := range b.CacheMessage {
		cacheList = append(cacheList, v)
	}
	return cacheList
}

func (b *BaseTask) ClearCacheList() {
	b.CacheMessage = make([]Message[any], 0)

}

func (b *BaseTask) AppendCacheList(message Message[any]) {
	b.CacheMessage = append(b.CacheMessage, message)

}

func (b *BaseTask) Open(controller *StatusController) {}

func (b *BaseTask) Process(message Message[any], controller *StatusController) {}

func (b *BaseTask) Close(controller *StatusController) {}

func (b *BaseTask) Run(ctx context.Context) {}

func (b *BaseTask) SinkRun(ctx context.Context) {}

//type Task[I any, O any] struct {
//	Receive chan Message[I]
//	Send    chan Message[O]
//
//	CacheMessage       []Message[I] // 只会在ck时触发
//	RegisterStatusName []string     // 注册状态变量名称
//
//	// 统计指标记录
//	receiveNumber       int64
//	sendNumber          int64
//	minuteReceiveNumber int64
//	hourReceiveNumber   int64
//	dayReceiveNumber    int64
//}
//
//func (t Task[I, O]) Open(controller *controller.StatusController) {
//
//}
//
//func (t Task[I, O]) Process(message Message[I], controller *controller.StatusController) {
//	//
//
//}
