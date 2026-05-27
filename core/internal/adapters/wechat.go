package adapters

import (
	"fmt"

	"recall/core/internal/model"
)

// WeChatAdapter is a compliance-safe placeholder for user-provided exports.
type WeChatAdapter struct{}

// NewWeChatAdapter creates the WeChat adapter placeholder.
func NewWeChatAdapter() *WeChatAdapter {
	return &WeChatAdapter{}
}

// ID returns the stable adapter identifier.
func (a *WeChatAdapter) ID() string {
	return "wechat"
}

// Name returns a human-readable adapter name.
func (a *WeChatAdapter) Name() string {
	return "WeChat"
}

// IsAvailable returns false until a user-authorized export importer is configured.
func (a *WeChatAdapter) IsAvailable() bool {
	return false
}

// StartSync refuses memory key extraction and database circumvention.
func (a *WeChatAdapter) StartSync() error {
	return fmt.Errorf("wechat memory scanning and encrypted database key extraction are not implemented")
}

// StopSync is a no-op for the placeholder.
func (a *WeChatAdapter) StopSync() error {
	return nil
}

// GetIncrementalData refuses protected database extraction.
func (a *WeChatAdapter) GetIncrementalData(lastSyncTime int64) ([]model.DataItem, error) {
	return nil, fmt.Errorf("wechat import requires a user-authorized export; protected database bypass is not supported")
}
