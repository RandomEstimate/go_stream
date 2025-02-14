package controller

import (
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
)

type BaseStruct struct {
	A int
	B string
}

type TestStruct struct {
	A int
	B string
}

func TestStatusController(t *testing.T) {

	/*
		测试思路

		生成测试思路
		生成状态控制器
		生成一些模拟的中间变量以及公共变量
		然后进行注册
		然后模拟进行一些加工和修改
		然后startCheckpoint
		一个一个提交

		加载测试思路
		有了目标ck文件
		先生成对应的中间变量以及公共变量进行注册
		然后进行加载，加载完成后进行一些操作
		然后startCheckpoint
		一个一个提交
	*/

	controller := NewController()

	// 生成一些变量
	base1 := 1
	base2 := "x"
	base3 := BaseStruct{
		A: 0,
		B: "x",
	}

	base4 := make([]int, 0, 10)
	base4 = append(base4, 1)

	entity1 := make(map[string]any)
	entity1["entity1_A1"] = 1
	entity1["entity1_A2"] = float64(2.0)
	entity1["entity1_A3"] = "xxx"

	entity2 := make(map[string]any)
	entity2["entity2_A1"] = 1
	entity2["entity2_A2"] = TestStruct{
		A: 0,
		B: "yyy",
	}
	entity2["entity2_A3"] = make(map[string]any)
	entity2["entity2_A3"].(map[string]any)["B"] = TestStruct{
		A: 1,
		B: "zzz",
	}

	// 开始注册
	controller.Register("base1", &base1)
	controller.Register("base2", &base2)
	controller.Register("base3", &base3)
	controller.Register("base4", &base4)
	controller.Register("entity1", &entity1)
	controller.Register("entity2", &entity2)
	gob.Register(BaseStruct{})
	gob.Register(TestStruct{})

	// 进行一些操作
	base1 += 1
	base2 += "x"
	base3.A += 1
	base3.B += "x"

	for i := 0; i < 100000; i++ {
		base4 = append(base4, 1)
	}

	// 直接更新是否会发生改变
	entity1["entity1_A1"] = entity1["entity1_A1"].(int) + 1
	entity1["entity1_A2"] = entity1["entity1_A2"].(float64) + 1.0
	entity1["entity1_A3"] = entity1["entity1_A3"].(string) + "yyy"

	// 取下来查看是否会发生改变
	struct_ := entity2["entity2_A2"].(TestStruct)
	struct_.A += 1

	w := sync.WaitGroup{}
	w.Add(1)
	go controller.startCheckpoint("c:/tmp/checkpoint/", 1, 60*60*1000, &w)

	name := []string{"base1", "base2", "base3", "base4", "entity1", "entity2"}
	controller.Checkpoint(name)
	w.Wait()

	fmt.Println("ck完成")

	base4 = append(base4[0:1], base4[10000+1:]...)

	// 读取ck 检查
	controller.loadCheckpoint("c:/tmp/checkpoint/checkpoint-1.gob", "c:/tmp/checkpoint/checkpoint-1-public.gob")
	fmt.Println("base1", base1)
	fmt.Println("base2", base2)
	fmt.Println("base3", base3)
	//fmt.Println("base4", base4)
	fmt.Println("base4 len ", len(base4))
	fmt.Println("entity1", entity1)
	fmt.Println("entity2", entity2)

	/*
		base1 2
		base2 xx
		base3 {1 xx}
		entity1 map[entity1_A1:2 entity1_A2:3 entity1_A3:xxxyyy]
		entity2 map[entity2_A1:1 entity2_A2:{0 yyy} entity2_A3:map[B:{1 zzz}]]
	*/

}

func TestStatusControllerBigData(t *testing.T) {
	controller := NewController()

	entity1 := make(map[string]float64)
	for i := 0; i < 10000000; i++ {
		entity1[fmt.Sprintf("entity_%d", i)] = float64(i)
	}

	controller.Register("entity1", &entity1)

	w := sync.WaitGroup{}
	w.Add(1)
	go controller.startCheckpoint("c:/tmp/checkpoint/", 1, 60*60*1000, &w)

	name := []string{"entity1"}
	controller.Checkpoint(name)
	w.Wait()

	fmt.Println("ck完成")

	// 大约200MB

}
