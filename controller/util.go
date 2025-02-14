package controller

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"path/filepath"
)

// isTypeRegistered 判断序列化是否被注册
func isTypeRegistered(value any) bool {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	return err == nil
}

// createDir 创建文件夹
func createDir(dirPath string) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		panic(fmt.Sprintf("创建文件夹失败: %v\n", err))
	}
}

// deleteFileInDir 删除文件夹下所有文件
func deleteFileInDir(dirPath string) {
	// 读取目录内的所有文件和子目录
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		panic(fmt.Sprintf("获取文件错误 %v", err))
	}

	// 遍历文件和子目录
	for _, file := range files {
		// 构建文件的完整路径
		filePath := filepath.Join(dirPath, file.Name())

		// 检查是否是文件
		if file.IsDir() {
			// 如果是目录，递归删除目录内的所有文件
			deleteFileInDir(filePath)
			// 删除空目录
			if err := os.Remove(filePath); err != nil {
				panic(fmt.Sprintf("删除空目录错误 %v", err))
			}
		} else {
			// 如果是文件，直接删除
			if err := os.Remove(filePath); err != nil {
				panic(fmt.Sprintf("删除文件夹错误 %v", err))
			}
		}
	}
}

// 生成目标文件目录
func createUniqueSubDir(parentDir string) (string, error) {
	// 生成一个UUID作为文件夹名称
	uniqueDirName := uuid.New().String()

	// 构建完整的子文件夹路径
	subDirPath := filepath.Join(parentDir, uniqueDirName)

	// 检查目录是否已经存在
	if _, err := os.Stat(subDirPath); !os.IsNotExist(err) {
		// 如果目录已经存在，递归调用自身重新生成
		return createUniqueSubDir(parentDir)
	}

	// 创建子文件夹
	err := os.Mkdir(subDirPath, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}

	return subDirPath, nil
}
