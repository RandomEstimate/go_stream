package controller

/**
Task 传递的基础消息
*/

type Message[T any] struct {
	Ts       int64  `json:"ts"`       // 消息时间戳
	IsCk     bool   `json:"isCk"`     // 是否是Ck消息
	Data     T      `json:"data"`     // 真实文本消息
	LastTask string `json:"lastTask"` // 上一个处理task名称
	Key      string `json:"key"`      // 绑定传输到下游的Key

	// sink端为了实现精准一次, 可以支持给自己的接收模块发送消息，当自己的接收模块收到消息，自己触发在process的
	// 定时sink任务，从而达到在process中更新状态的目的，实现sink端只有写入完成才算ck完成。
	IsInner bool `json:"record"` // 其他标志位
}

// NewMessage 消息构造器
func NewMessage[T any](ts int64, isCk bool, data T, lastTask string, key string) *Message[T] {
	return &Message[T]{Ts: ts, IsCk: isCk, Data: data, LastTask: lastTask, Key: key, IsInner: false}
}

// Copy 复制消息
func (a *Message[T]) Copy() *Message[T] {
	copyMessage := NewMessage(a.Ts, a.IsCk, a.Data, a.LastTask, a.Key)
	if a.IsCk == true {
		copyMessage.IsCk = false
	}
	return copyMessage
}
