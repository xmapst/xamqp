package xamqp

import (
	"fmt"
	"log"

	"github.com/xmapst/xamqp/internal/logger"
)

// ILogger xamqp 包对外暴露的日志接口，与内部 logger.ILogger 保持一致。
//
// 通过类型别名暴露内部接口，外部调用方可直接注入自定义实现，
// 例如注入 xlog 的 ILogger 实现，使 xamqp 的日志与业务系统统一。
type ILogger logger.ILogger

// loggingPrefix 默认日志输出前缀，用于区分 xamqp 包的日志。
const loggingPrefix = "gorabbit"

// stdDebugLogger 基于标准库 log 包的默认日志实现。
//
// 将所有日志输出到标准错误，格式为 "gorabbit {LEVEL}: {message}"。
// 生产环境建议通过 With*OptionsLogger 注入与业务系统一致的日志实现。
type stdDebugLogger struct{}

// Errorf 以 ERROR 级别输出格式化日志。
func (l stdDebugLogger) Errorf(format string, v ...any) {
	log.Printf(fmt.Sprintf("%s ERROR: %s", loggingPrefix, format), v...)
}

// Warnf 以 WARN 级别输出格式化日志。
func (l stdDebugLogger) Warnf(format string, v ...any) {
	log.Printf(fmt.Sprintf("%s WARN: %s", loggingPrefix, format), v...)
}

// Infof 以 INFO 级别输出格式化日志。
func (l stdDebugLogger) Infof(format string, v ...any) {
	log.Printf(fmt.Sprintf("%s INFO: %s", loggingPrefix, format), v...)
}

// Debugf 以 DEBUG 级别输出格式化日志。
func (l stdDebugLogger) Debugf(format string, v ...any) {
	log.Printf(fmt.Sprintf("%s DEBUG: %s", loggingPrefix, format), v...)
}
