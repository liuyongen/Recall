package adapters

import (
	"fmt"

	"recall/core/internal/model"
)

// WeChatAdapter 是面向用户自提供导出的合规安全占位适配器。
type WeChatAdapter struct{}

// NewWeChatAdapter 创建微信适配器占位实现。
func NewWeChatAdapter() *WeChatAdapter {
	return &WeChatAdapter{}
}

// ID 返回稳定的适配器标识。
func (a *WeChatAdapter) ID() string {
	return "wechat"
}

// Name 返回适合用户阅读的适配器名称。
func (a *WeChatAdapter) Name() string {
	return "WeChat"
}

// IsAvailable 在配置用户授权的导入器前始终返回 false。
func (a *WeChatAdapter) IsAvailable() bool {
	return false
}

// StartSync 拒绝内存密钥提取和数据库绕过。
func (a *WeChatAdapter) StartSync() error {
	return fmt.Errorf("wechat memory scanning and encrypted database key extraction are not implemented")
}

// StopSync 对占位实现无操作。
func (a *WeChatAdapter) StopSync() error {
	return nil
}

// GetIncrementalData 拒绝提取受保护数据库。
func (a *WeChatAdapter) GetIncrementalData(lastSyncTime int64) ([]model.DataItem, error) {
	return nil, fmt.Errorf("wechat import requires a user-authorized export; protected database bypass is not supported")
}
