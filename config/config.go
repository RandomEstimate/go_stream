package config

// checkpoint 配置

var CheckpointInterval int64 = 60 * 60 * 100
var CheckpointNumber int = 1
var CheckpointTimeout int64 = 30 * 60 * 100
var CheckpointPath string = ""

// 启动状态配置

var StartMode = "standard"        // standard | checkpoint
var CheckpointFilePath = ""       // checkpoint 模式下文件路径
var CheckpointPublicFilePath = "" // checkpoint 模式下文件路径
