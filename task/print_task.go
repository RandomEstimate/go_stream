package task

import (
	"fmt"
	"go_stream/controller"
)

type PrintTask struct {
	controller.BaseTask
}

func NewPrintTask() *PrintTask {
	return &PrintTask{}
}

func (p *PrintTask) Process(message controller.Message[any], controller *controller.StatusController) {
	fmt.Println(message.Data)
}
