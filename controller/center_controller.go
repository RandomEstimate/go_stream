package controller

import (
	"context"
	"fmt"
	"github.com/RandomEstimate/util_go/common"
	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
	"github.com/sirupsen/logrus"
	"go_stream/config"
	"go_stream/logger"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// 中央控制器
/*
管理所有的task 以及task的启动、添加、删除、定义拓扑结构
管理source task 控制checkpoint
*/

type CenterController struct {
	m                sync.Mutex
	statusController *StatusController
	taskMap          map[string]*taskInfo
	createTaskFunc   map[string]func(map[string]string) (*Task, error) // 存储生成task的方法, 运行时可以动态生成task

	checkpointInterval int64        // ck间隔时间
	checkpointStatus   atomic.Value // 当前是否为checkpoint状态
	checkpointNumber   int          // ck 编号
	checkpointPath     string       // checkpoint 目录地址
	checkpointTimeout  int64        // checkpoint超时时间
}

type taskInfo struct {
	name          string     // Task名称
	t             *Task      // Task 实体
	receive       []string   // 接收
	send          []string   // 发送
	receiveKey    string     // 接收Key
	receiveType   string     // 接收类型
	sendType      string     // 发送类型
	taskType      string     // 任务类型：数据源、普通、数据终端三种 | source、common、sink
	m             sync.Mutex // 锁的目的在于修改接收 发送时必须单线程进行操作
	ctx           context.Context
	cancelFunc    context.CancelFunc
	runningStatus atomic.Value // 运行状态 bool

	// 统计指标记录
	statistic Statistic

	receiveDataTime []time.Time // 记录一下每个数据到达当前process时间
	sendDataTime    []time.Time // 记录一下每个数据到达当前发送时间
	delayTime       []int64     // 记录处理延时

	statisticM sync.Mutex // 统计指标锁
}

type Statistic struct {
	Name                string // Task名称
	ReceiveNumber       int64  // 当前到达数量
	SendNumber          int64  // 当前发送数量
	MinuteReceiveNumber int64  // 每分钟收到数量
	HourReceiveNumber   int64  // 每小时收到数量
	DayReceiveNumber    int64  // 每天收到数量
	MinuteSendNumber    int64  // 每分钟发送数量
	HourSendNumber      int64  // 每小时发送数量
	DaySendNumber       int64  // 每天发送数量
	DelayTimeMill       int64  // 近100个数据处理平均延时
}

func NewCenterController() *CenterController {
	checkpointStatus := atomic.Value{}
	checkpointStatus.Store(false)
	return &CenterController{
		m:                sync.Mutex{},
		statusController: NewController(),
		taskMap:          make(map[string]*taskInfo),
		createTaskFunc:   make(map[string]func(map[string]string) (*Task, error)),

		checkpointInterval: config.CheckpointInterval,
		checkpointStatus:   checkpointStatus,
		checkpointNumber:   1,
		checkpointPath:     config.CheckpointPath,
		checkpointTimeout:  config.CheckpointTimeout,
	}
}

func (t *taskInfo) addReceive(name string) {
	t.m.Lock()
	defer t.m.Unlock()

	index := common.Index(name, t.receive)
	if index != -1 {
		panic(fmt.Sprintf("task[%s], addReceive存在重复[%s]\n", t.name, name))
	}

	t.receive = append(t.receive, name)
}

func (t *taskInfo) addSend(name string) {
	t.m.Lock()
	defer t.m.Unlock()

	index := common.Index(name, t.send)
	if index != -1 {
		panic(fmt.Sprintf("task[%s], addSend存在重复[%s]\n", t.name, name))
	}

	t.send = append(t.send, name)
}

func (t *taskInfo) deleteReceive(name string) {
	t.m.Lock()
	defer t.m.Unlock()

	index := common.Index(name, t.receive)
	if index == -1 {
		panic(fmt.Sprintf("task[%s], deleteReceive删除不存在[%s]\n", t.name, name))
	}
	t.receive = append(t.receive[:index], t.receive[index+1:]...)

}

func (t *taskInfo) checkDelete() bool {
	t.m.Lock()
	defer t.m.Unlock()

	if len(t.receive) != 0 || len(t.send) != 0 {
		return false
	}

	return true
}

func (t *taskInfo) deleteSend(name string) {
	t.m.Lock()
	defer t.m.Unlock()

	index := common.Index(name, t.send)
	if index == -1 {
		panic(fmt.Sprintf("task[%s], deleteSend删除不存在[%s]\n", t.name, name))
	}
	t.send = append(t.send[:index], t.send[index+1:]...)

}

func (c *CenterController) waitCheckpoint() {
	for {
		// 采用自旋
		if c.checkpointStatus.Load() == true {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

// RegisterTask 注册Task
func (c *CenterController) RegisterTask(name string, t Task, receiveKey string, taskType string) {
	c.waitCheckpoint()

	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.taskMap[name]; ok {
		panic(fmt.Sprintf("task名称必须唯一, 当前注册名称为[%s]\n", name))
	}

	if taskType != "source" && taskType != "common" && taskType != "sink" {
		panic(fmt.Sprintf("task类型必须为 source、common、sink\n"))
	}

	ctx, cancel := context.WithCancel(context.Background())

	runningStatus := atomic.Value{}
	runningStatus.Store(false)
	info := taskInfo{
		name:          name,
		t:             &t,
		receive:       make([]string, 0),
		send:          make([]string, 0),
		receiveKey:    receiveKey,
		receiveType:   t.GetReceiveType().Name(),
		sendType:      t.GetSendType().Name(),
		taskType:      taskType,
		m:             sync.Mutex{},
		ctx:           ctx,
		cancelFunc:    cancel,
		runningStatus: runningStatus,

		// 初始化统计指标
		statistic: Statistic{
			Name:                name,
			ReceiveNumber:       0,
			SendNumber:          0,
			MinuteReceiveNumber: 0,
			HourReceiveNumber:   0,
			DayReceiveNumber:    0,
			MinuteSendNumber:    0,
			HourSendNumber:      0,
			DaySendNumber:       0,
			DelayTimeMill:       0,
		},
		receiveDataTime: make([]time.Time, 0),
		sendDataTime:    make([]time.Time, 0),
		delayTime:       make([]int64, 0),
		statisticM:      sync.Mutex{},
	}

	if taskType == "source" {
		info.receive = append(info.receive, "center")
	}
	c.taskMap[name] = &info

	logger.Log.WithFields(logrus.Fields{
		"center": "RegisterTask",
	}).Info(fmt.Sprintf("中央控制器完成Task注册: name[%s], receiveKey[%s], taskType[%s]", name, receiveKey, taskType))

}

// Connect 连接Task
func (c *CenterController) Connect(startTask string, endTask string) {

	c.waitCheckpoint()

	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.taskMap[startTask]; !ok {
		panic(fmt.Sprintf("不存在task [%s], 连接失败\n", startTask))
	}
	if _, ok := c.taskMap[endTask]; !ok {
		panic(fmt.Sprintf("不存在task [%s], 连接失败\n", endTask))
	}

	startTaskInfo := c.taskMap[startTask]
	endTaskInfo := c.taskMap[endTask]

	if startTaskInfo.sendType != endTaskInfo.receiveType {
		panic(fmt.Sprintf("连接类型不一致 startTask类型[%s], endTask类型[%s]\n", startTaskInfo.sendType, endTaskInfo.receiveType))
	}

	startTaskInfo.addSend(endTask)
	endTaskInfo.addReceive(startTask)

	logger.Log.WithFields(logrus.Fields{
		"center": "Connect",
	}).Info(fmt.Sprintf("中央控制器完成Task连接: startTask[%s], endTask[%s]", startTask, endTask))
}

// DisConnect 取消Task连接
func (c *CenterController) DisConnect(startTask string, endTask string) {
	c.waitCheckpoint()

	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.taskMap[startTask]; !ok {
		panic(fmt.Sprintf("不存在task [%s], 取消连接失败\n", startTask))
	}
	if _, ok := c.taskMap[endTask]; !ok {
		panic(fmt.Sprintf("不存在task [%s], 取消连接失败\n", endTask))
	}

	startTaskInfo := c.taskMap[startTask]
	endTaskInfo := c.taskMap[endTask]

	startTaskInfo.deleteSend(endTask)
	endTaskInfo.deleteReceive(startTask)

	logger.Log.WithFields(logrus.Fields{
		"center": "DisConnect",
	}).Info(fmt.Sprintf("中央控制器完成Task断开连接: startTask[%s], endTask[%s]", startTask, endTask))

}

// DeleteTask 删除task
func (c *CenterController) DeleteTask(name string) {
	c.waitCheckpoint()

	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.taskMap[name]; !ok {
		panic(fmt.Sprintf("不存在task [%s], 删除失败\n", name))
	}

	if c.taskMap[name].checkDelete() {
		c.taskMap[name].cancelFunc()
		delete(c.taskMap, name)
	} else {
		logger.Log.WithFields(logrus.Fields{
			"center": "DeleteTask",
		}).Warn(fmt.Sprintf("中央控制器Task删除失败, 依然存在连接情况下删除: name[%s]", name))
	}

	logger.Log.WithFields(logrus.Fields{
		"center": "DeleteTask",
	}).Info(fmt.Sprintf("中央控制器完成Task删除: name[%s]", name))

}

// AddCreateTaskFunc 注册生成task的方法
func (c *CenterController) AddCreateTaskFunc(name string, function func(map[string]string) (*Task, error)) {
	c.createTaskFunc[name] = function

	logger.Log.WithFields(logrus.Fields{
		"center": "AddCreateTaskFunc",
	}).Info(fmt.Sprintf("中央控制器添加Task生产方法: name[%s]", name))

}

// DynamicsCreateTaskAndStart 动态创建Task注册并启动
func (c *CenterController) DynamicsCreateTaskAndStart(name string, taskName string,
	param map[string]string, receive []string, send []string, receiveKey string, taskType string) {

	if _, ok := c.createTaskFunc[name]; !ok {
		logger.Log.WithFields(logrus.Fields{
			"center": "DynamicsCreateTaskAndStart",
		}).Warn(fmt.Sprintf("中央控制器动态创建task, 注册生成方法不存在: name[%s]", name))
		return
	}

	f := c.createTaskFunc[name]

	task, err := f(param)
	if err != nil {
		fmt.Println(fmt.Sprintf("动态创建task, 创建失败 %v", err))
	}

	c.RegisterTask(taskName, *task, receiveKey, taskType)

	// 进行连接

}

func (c *CenterController) startTask(name string) {

	task := c.taskMap[name]

	// 注册状态变量
	taskObject := *task.t
	// 将注册状态在Start中执行
	// taskObject.Open(c.statusController)

	checkpointFinish := func(checkpointMap map[string]int) bool {
		flag := true
		for _, v := range checkpointMap {
			if v == 0 {
				flag = false
				break
			}
		}
		return flag
	}

	handleAfterCk := func(data Message[any]) {
		// 向后续task发送ck信息
		ckData := data.Copy()
		ckData.IsCk = true
		taskObject.SendMessage(*ckData)

		// 开始处理缓存队列
		cacheList := taskObject.GetCacheList()
		for i := 0; i < len(cacheList); i++ {
			taskObject.Process(cacheList[i], c.statusController)
		}

		taskObject.ClearCacheList()
		task.runningStatus.Store(false)
	}

	checkpoint := func(data Message[any]) {
		logger.Log.WithFields(logrus.Fields{
			"center": "startTask",
		}).Info(fmt.Sprintf("Task[%s] 触发checkpoint", name))

		c.statusController.Checkpoint(taskObject.GetRegisterStatusName())
		handleAfterCk(data)

		logger.Log.WithFields(logrus.Fields{
			"center": "startTask",
		}).Info(fmt.Sprintf("Task[%s] 完成checkpoint", name))
	}

	process := func(data Message[any]) {
		processStart := time.Now()
		taskObject.Process(data, c.statusController)
		task.statisticM.Lock()
		task.delayTime = append(task.delayTime, time.Since(processStart).Microseconds())
		task.statisticM.Unlock()
	}

	if task.taskType == "source" {
		go taskObject.Run(task.ctx)
	}

	if task.taskType == "sink" {
		go taskObject.SinkRun(task.ctx)
	}

	go task.handleStatistic()

	// 接收模块
	go func() {

		logger.Log.WithFields(logrus.Fields{
			"center": "startTask",
		}).Info(fmt.Sprintf("Task[%s] 开启接收", name))

		checkpointMap := make(map[string]int)
		for {
			select {
			case <-task.ctx.Done():
				// 执行退出命令
				logger.Log.WithFields(logrus.Fields{
					"center": "startTask",
				}).Info(fmt.Sprintf("Task[%s] 接收模块获取退出命令，准备执行自身退出指令", name))

				taskObject.Close(c.statusController)

				logger.Log.WithFields(logrus.Fields{
					"center": "startTask",
				}).Info(fmt.Sprintf("Task[%s] 接收模块获取退出命令，自身退出指令执行完毕", name))

				return

			case data := <-taskObject.GetReceiveMessageChan():

				if data.IsInner == false && data.IsCk == false {
					task.statisticM.Lock()
					task.statistic.ReceiveNumber += 1
					task.receiveDataTime = append(task.receiveDataTime, time.Now())
					task.statisticM.Unlock()
				}

				// 判断接收类型类型和定义类型是否一致
				if data.IsInner == false && data.IsCk == false && reflect.TypeOf(data.Data) != taskObject.GetReceiveType() {
					panic(fmt.Sprintf("task[%s] 接收数据类型和定义类型不一致, 接收类型[%s], 定义类型[%s]",
						task.name, reflect.TypeOf(data.Data).Name(), taskObject.GetReceiveType().Name()))

				}

				if data.IsCk && task.runningStatus.Load() == false {
					// 触发ck
					task.runningStatus.Store(true)
					// 遍历所有接收源
					task.m.Lock()
					for i := 0; i < len(task.receive); i++ {
						checkpointMap[task.receive[i]] = 0
						if task.receive[i] == data.LastTask {
							checkpointMap[task.receive[i]] = 1
						}
					}
					task.m.Unlock()

					// 检测是否ck完毕
					if checkpointFinish(checkpointMap) {
						checkpoint(data)
					}

				} else if data.IsCk && task.runningStatus.Load() == true {
					// 等待ck
					checkpointMap[data.LastTask] = 1

					if checkpointFinish(checkpointMap) {
						checkpoint(data)
					}

				} else if data.IsCk == false && task.runningStatus.Load() == true {
					if checkpointMap[data.LastTask] == 1 {
						// 加入缓存队列
						taskObject.AppendCacheList(data)
					} else {
						// 直接进行处理
						process(data)
					}
				} else if data.IsCk == false && task.runningStatus.Load() == false {
					// 正常处理
					process(data)
				}

			}
		}
	}()

	// 分发模块
	go func() {
		logger.Log.WithFields(logrus.Fields{
			"center": "startTask",
		}).Info(fmt.Sprintf("Task[%s] 开启分发", name))
		for {
			select {
			case <-task.ctx.Done():
				return
			case data := <-taskObject.GetSendMessageChan():

				if data.IsCk == false {
					task.statisticM.Lock()
					task.statistic.SendNumber += 1
					task.sendDataTime = append(task.sendDataTime, time.Now())
					task.statisticM.Unlock()
				}

				if data.IsCk == false && reflect.TypeOf(data.Data) != taskObject.GetSendType() {
					panic(fmt.Sprintf("task[%s] 发送数据类型和定义类型不一致, 发送类型[%s], 定义类型[%s]",
						task.name, reflect.TypeOf(data.Data).Name(), taskObject.GetSendType().Name()))
				}

				data.LastTask = task.name
				task.m.Lock()
				if data.IsCk {

					// 获取要发送的清单
					for i := 0; i < len(task.send); i++ {
						t := *c.taskMap[task.send[i]].t
						t.GetReceiveMessageChan() <- data
					}

				} else {
					// 获取发送满足要求Key的列表
					// 目前采用发送给所有订阅消息的下游task, 后续支持多任务并行处理的情况
					for i := 0; i < len(task.send); i++ {
						taskInfo := c.taskMap[task.send[i]]
						if taskInfo.receiveKey == data.Key {
							t := *taskInfo.t
							t.GetReceiveMessageChan() <- data
						}
					}
				}
				task.m.Unlock()

			}
		}
	}()
}

// handleCheckpoint 控制checkpoint
func (c *CenterController) handleCheckpoint() {

	timer := time.NewTicker(time.Duration(c.checkpointInterval) * time.Millisecond)

	for {
		select {
		case <-timer.C:
			// 发起checkpoint
			logger.Log.WithFields(logrus.Fields{
				"center": "handleCheckpoint",
			}).Info(fmt.Sprintf("中央控制器发起checkpoint"))

			c.checkpointStatus.Store(true)

			w := sync.WaitGroup{}
			w.Add(1)
			go c.statusController.startCheckpoint(c.checkpointPath, c.checkpointNumber, c.checkpointTimeout, &w)

			c.m.Lock()
			for k := range c.taskMap {
				if c.taskMap[k].taskType == "source" {
					t := *c.taskMap[k].t
					ckMessage := Message[any]{
						Ts:       0,
						IsCk:     true,
						LastTask: "center", // source 连接的是center
					}
					t.GetReceiveMessageChan() <- ckMessage
				}
			}
			c.m.Unlock()

			w.Wait()

			// 将ck number 加1
			c.checkpointNumber += 1
			c.checkpointStatus.Store(false)
			logger.Log.WithFields(logrus.Fields{
				"center": "handleCheckpoint",
			}).Info(fmt.Sprintf("中央控制器checkpoint完成, 当前阶段checkpointNumber[%d]", c.checkpointNumber-1))
		}
	}
}

func (t *taskInfo) handleStatistic() {
	timer := time.NewTicker(time.Second * 60) // 10s 进行一次统计
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-timer.C:
			t.statisticM.Lock()
			// 过滤掉24小时之前的记录， 统计最近1分钟 1小时 1天的数量
			// 并把新的记录保存
			receiveTmpList := make([]time.Time, 0)
			minIndicator := 0
			hourIndicator := 0
			dayIndicator := 0
			for i := 0; i < len(t.receiveDataTime); i++ {
				if time.Since(t.receiveDataTime[i]) < 24*time.Hour {
					receiveTmpList = append(receiveTmpList, t.receiveDataTime[i])
				}

				if time.Since(t.receiveDataTime[i]) < 24*time.Hour {
					dayIndicator += 1
				}

				if time.Since(t.receiveDataTime[i]) < time.Hour {
					hourIndicator += 1
				}

				if time.Since(t.receiveDataTime[i]) < time.Minute {
					minIndicator += 1
				}
			}

			t.statistic.MinuteReceiveNumber = int64(minIndicator)
			t.statistic.HourReceiveNumber = int64(hourIndicator)
			t.statistic.DayReceiveNumber = int64(dayIndicator)
			t.receiveDataTime = receiveTmpList

			sendTmpList := make([]time.Time, 0)
			minIndicator = 0
			hourIndicator = 0
			dayIndicator = 0
			for i := 0; i < len(t.sendDataTime); i++ {
				if time.Since(t.sendDataTime[i]) < 24*time.Hour {
					sendTmpList = append(sendTmpList, t.sendDataTime[i])
				}

				if time.Since(t.sendDataTime[i]) < 24*time.Hour {
					dayIndicator += 1
				}

				if time.Since(t.sendDataTime[i]) < time.Hour {
					hourIndicator += 1
				}

				if time.Since(t.sendDataTime[i]) < time.Minute {
					minIndicator += 1
				}
			}

			t.statistic.MinuteSendNumber = int64(minIndicator)
			t.statistic.HourSendNumber = int64(hourIndicator)
			t.statistic.DaySendNumber = int64(dayIndicator)
			t.sendDataTime = sendTmpList

			if len(t.delayTime) > 100 {
				t.delayTime = t.delayTime[len(t.delayTime)-100:]
			}

			sum := int64(0)
			for i := 0; i < len(t.delayTime); i++ {
				sum += t.delayTime[i]
			}

			t.statistic.DelayTimeMill = int64(float64(sum) / float64(len(t.delayTime)))

			t.statisticM.Unlock()
		}
	}
}

func (c *CenterController) GetTaskStatistic(name string) (Statistic, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if t, ok := c.taskMap[name]; ok {
		t.statisticM.Lock()
		defer t.statisticM.Unlock()
		return t.statistic, nil
	} else {
		return Statistic{}, fmt.Errorf("无法查询到名称为[%s]的task", name)
	}

}

func (c *CenterController) GetGraph() {
	c.m.Lock()
	defer c.m.Unlock()

	// 开始画图
	g := graphviz.New()
	graph, _ := g.Graph()
	defer func() {
		graph.Close()
		g.Close()
	}()

	// 设置图的布局方向为从左到右
	graph.SetRankDir(cgraph.LRRank)

	// 定义节点
	nodeMap := make(map[string]*cgraph.Node)
	for k := range c.taskMap {
		node, _ := graph.CreateNode(k)
		if c.taskMap[k].taskType == "source" || c.taskMap[k].taskType == "sink" {
			node.SetColor("red")
		}
		node.SetShape(cgraph.BoxShape)
		nodeMap[k] = node
	}

	// 定义边
	for k := range c.taskMap {
		t := c.taskMap[k]
		if t.taskType != "source" {
			for _, name := range t.receive {
				graph.CreateEdge(name+"_to_"+k, nodeMap[name], nodeMap[k])
			}
		}

		if t.taskType != "sink" {
			for _, name := range t.send {
				graph.CreateEdge(k+"_to_"+name, nodeMap[k], nodeMap[name])
			}
		}
	}

	g.RenderFilename(graph, graphviz.PNG, "graph.png")
}

// Start 启动
func (c *CenterController) Start() {
	// 对基本信息进行检测

	// checkpoint 配置检测
	if c.checkpointInterval <= 0 || c.checkpointTimeout <= 0 {
		panic("checkpointInterval | checkpointTimeout 需要配置>=0")
	}

	if c.checkpointStatus.Load() == true {
		c.checkpointStatus.Store(false)
	}

	// 检测ck地址
	// 如果路径错误会直接panic
	createDir(c.checkpointPath)

	// 不再需要source节点检测 可以存在多个source情况
	c.m.Lock()

	// 注册变量
	for k := range c.taskMap {
		t := *c.taskMap[k].t
		t.Open(c.statusController)
	}

	// 如果是checkpoint模式启动则需要进行状态装载同时更新checkpoint number (也可以不用更新下一次从1开始)
	if config.StartMode == "checkpoint" {
		c.statusController.loadCheckpoint(config.CheckpointFilePath, config.CheckpointPublicFilePath)
	}

	for k := range c.taskMap {
		c.startTask(k)
	}

	c.m.Unlock()

	// 启动单独处理checkpoint程序
	go c.handleCheckpoint()

	select {}

}
