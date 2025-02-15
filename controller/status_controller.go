package controller

import (
	"encoding/gob"
	"fmt"
	"github.com/RandomEstimate/go_stream/logger"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
)

// 状态控制器

type StatusController struct {
	m              sync.Mutex     // 锁
	statusMap      map[string]any // 状态变量
	ckStatusMap    map[string]any // checkpoint临时map
	ckBackCallChan chan struct{}  // checkpoint回调chan

	publicMap map[string]string // 用于存储一些公共变量
	publicM   sync.Mutex
}

func NewController() *StatusController {
	return &StatusController{
		m:              sync.Mutex{},
		statusMap:      make(map[string]any),
		ckStatusMap:    make(map[string]any),
		ckBackCallChan: make(chan struct{}, 1),

		publicMap: make(map[string]string),
		publicM:   sync.Mutex{},
	}
}

// Register 向控制器注册状态
func (c *StatusController) Register(key string, value any) {
	val := reflect.ValueOf(value)
	if val.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("key[%s] 注册状态必须为指针", key))
	}

	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.statusMap[key]; ok {
		panic(fmt.Sprintf("key[%s] 注册键必须唯一", key))
	}

	// 注册时不应该采用指针
	//reflectValue := reflect.ValueOf(value).Elem().Interface()
	//if !isTypeRegistered(reflectValue) {
	//	gob.Register(reflectValue)
	//}

	gob.Register(reflect.ValueOf(value).Elem().Interface())
	//gob.Register(reflect.TypeOf(value).Elem())

	c.statusMap[key] = value
}

func (c *StatusController) GetPublicValue(key string) (string, error) {
	c.publicM.Lock()
	defer c.publicM.Unlock()

	if _, ok := c.publicMap[key]; ok {
		return c.publicMap[key], nil
	}

	return "", fmt.Errorf("key [%s] 不存在", key)

}

func (c *StatusController) SetPublicValue(key string, value string) {
	c.publicM.Lock()
	defer c.publicM.Unlock()

	c.publicMap[key] = value
}

func (c *StatusController) DeletePublicValue(key string) {
	c.publicM.Lock()
	defer c.publicM.Unlock()

	delete(c.publicMap, key)
}

// ReadSnapshotStatus 向控制器读取其他状态副本，不保证一致性
func (c *StatusController) ReadSnapshotStatus(key string) any {
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.statusMap[key]; !ok {
		panic(fmt.Sprintf("key[%s] 读取状态不存在", key))
	}
	val := reflect.ValueOf(c.statusMap[key])
	if val.Kind() == reflect.Ptr {
		elem := val.Elem()
		copiedValue := reflect.New(elem.Type()).Elem()
		copiedValue.Set(elem)
		return copiedValue
	}

	panic(fmt.Sprintf("key[%s] 读取状态不为指针", key))

}

// Checkpoint 其他Task向控制器发送ck指令
func (c *StatusController) Checkpoint(key []string) {
	c.m.Lock()
	defer c.m.Unlock()
	for _, k := range key {
		if _, ok := c.ckStatusMap[k]; ok {
			panic(fmt.Sprintf("checkpoint出现重复key[%s], 可以尝试查看是否超时导致.", k))
		}
		if reflectValue := reflect.ValueOf(c.statusMap[k]); reflectValue.Kind() == reflect.Ptr {
			newValue := reflect.New(reflectValue.Elem().Type())
			newValue.Elem().Set(reflectValue.Elem())
			c.ckStatusMap[k] = newValue.Interface()
		}
	}

	flag := true
	for k, _ := range c.statusMap {
		if _, ok := c.ckStatusMap[k]; !ok {
			flag = false
			break
		}
	}

	if flag {
		c.ckBackCallChan <- struct{}{}
	}

}

func (c *StatusController) startCheckpoint(checkpointPath string, checkpointNumber int, timeOut int64, w *sync.WaitGroup) {
	timer := time.After(time.Millisecond * time.Duration(timeOut))
	for {
		select {
		case <-c.ckBackCallChan:
			// 所有checkpoint已经完成
			c.writeCheckpoint(checkpointPath, checkpointNumber)
			c.ckStatusMap = make(map[string]any)
			w.Done()
			return
		case <-timer:
			panic(fmt.Sprintf("checkpoint已经超时, 设置超时为[%d]", timeOut))
		}
	}
}

func (c *StatusController) writeCheckpoint(checkpointPath string, checkpointNumber int) {

	logger.Log.WithFields(logrus.Fields{
		"status": "writeCheckpoint",
	}).Info(fmt.Sprintf("checkpoint记录开始"))

	subDir, err := createUniqueSubDir(os.TempDir())
	if err != nil {
		panic(fmt.Sprintf("writeCheckpoint 生成目标文件目录出错 %v", err))
	}

	filePathTmp := subDir + "/" + "checkpoint-" + fmt.Sprint(checkpointNumber) + ".gob"
	file, err := os.Create(filePathTmp)
	if err != nil {
		panic(fmt.Sprintf("writeCheckpoint 创建文件错误 %v", err))
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(c.ckStatusMap)
	if err != nil {
		panic(fmt.Sprintf("Error encoding to gob: %v", err))
	}
	file.Close()

	// 记录公共变量
	c.publicM.Lock()
	publicFilePathTmp := subDir + "/" + "checkpoint-" + fmt.Sprint(checkpointNumber) + "-public.gob"
	file, err = os.Create(publicFilePathTmp)
	if err != nil {
		panic(fmt.Sprintf("writeCheckpoint 创建文件错误 %v", err))
	}
	encoder = gob.NewEncoder(file)
	err = encoder.Encode(c.publicMap)
	if err != nil {
		panic(fmt.Sprintf("Error encoding to gob: %v", err))
	}
	file.Close()
	c.publicM.Unlock()

	logger.Log.WithFields(logrus.Fields{
		"status": "writeCheckpoint",
	}).Info(fmt.Sprintf("checkpoint 写入完成, 写入地址[%s] [%s]", filePathTmp, publicFilePathTmp))

	filePath := checkpointPath + "/" + "checkpoint-" + fmt.Sprint(checkpointNumber) + ".gob"
	publicFilePath := checkpointPath + "/" + "checkpoint-" + fmt.Sprint(checkpointNumber) + "-public.gob"
	dirPath := filepath.Dir(filePath)
	createDir(dirPath)
	deleteFileInDir(dirPath)

	if err := os.Rename(filePathTmp, filePath); err != nil {
		panic(fmt.Sprintf("writeCheckpoint 移动文件失败: %v\n", err))
	}

	if err := os.Rename(publicFilePathTmp, publicFilePath); err != nil {
		panic(fmt.Sprintf("writeCheckpoint 移动文件失败: %v\n", err))
	}

	logger.Log.WithFields(logrus.Fields{
		"status": "writeCheckpoint",
	}).Info(fmt.Sprintf("checkpoint 成功写入目标地址[%s] [%s]", filePath, publicFilePath))

	os.RemoveAll(subDir)

}

func (c *StatusController) loadCheckpoint(checkpointFilePath string, checkpointPublicFilePath string) {
	if _, err := os.Stat(checkpointFilePath); os.IsNotExist(err) {
		panic(fmt.Sprintf("loadCheckpoint 源文件不存在: %v\n", err))
	}

	file, err := os.Open(checkpointFilePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	c.ckStatusMap = make(map[string]any)
	err = decoder.Decode(&c.ckStatusMap)
	if err != nil {
		panic(fmt.Sprintf("loadCheckpoint 无法解析checkpoint文件 %v", err))
	}

	// 需要两个map完全一致
	if len(c.statusMap) != len(c.ckStatusMap) {
		panic("loadCheckpoint 检测到注册状态与加载状态长度不一致")
	}
	for key := range c.statusMap {
		if _, exists := c.ckStatusMap[key]; !exists {
			panic(fmt.Sprintf("loadCheckpoint 检测到注册状态与加载状态存在不一致状态名称 %s", key))
		}
	}

	// 将指针重新指向新的对象
	for key := range c.ckStatusMap {
		v := reflect.ValueOf(c.statusMap[key])
		v.Elem().Set(reflect.ValueOf(c.ckStatusMap[key]))
	}

	c.ckStatusMap = make(map[string]any)

	// 读取公共变量
	if _, err := os.Stat(checkpointPublicFilePath); os.IsNotExist(err) {
		panic(fmt.Sprintf("loadCheckpoint 源文件不存在: %v\n", err))
	}

	file2, err := os.Open(checkpointPublicFilePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file2.Close()
	decoder2 := gob.NewDecoder(file2)
	err = decoder2.Decode(&c.publicMap)
	if err != nil {
		panic(fmt.Sprintf("loadCheckpoint 无法解析checkpoint文件 %v", err))
	}
}
