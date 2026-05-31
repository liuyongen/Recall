package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"recall/core/internal/model"
)

// BrowserKind 标识受支持的本地浏览器配置。
type BrowserKind string

const (
	// BrowserChrome 表示 Google Chrome。
	BrowserChrome BrowserKind = "chrome"
	// BrowserEdge 表示 Microsoft Edge。
	BrowserEdge BrowserKind = "edge"
	// BrowserFirefox 表示 Mozilla Firefox。
	BrowserFirefox BrowserKind = "firefox"
)

// BrowserAdapter 从浏览器配置文件读取本地历史和书签。
type BrowserAdapter struct {
	Kind BrowserKind
}

// NewBrowserAdapter 为一个浏览器系列创建适配器。
func NewBrowserAdapter(kind BrowserKind) *BrowserAdapter {
	return &BrowserAdapter{Kind: kind}
}

// ID 返回稳定的适配器标识。
func (a *BrowserAdapter) ID() string {
	return "browser." + string(a.Kind)
}

// Name 返回适合用户阅读的适配器名称。
func (a *BrowserAdapter) Name() string {
	return titleCaseASCII(string(a.Kind)) + " History"
}

// IsAvailable 判断本地是否存在浏览器配置数据。
func (a *BrowserAdapter) IsAvailable() bool {
	for _, path := range a.historyPaths() {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}
	return false
}

// StartSync 无操作，因为浏览器文件通过增量轮询读取。
func (a *BrowserAdapter) StartSync() error {
	return nil
}

// StopSync 无操作，因为浏览器文件通过增量轮询读取。
func (a *BrowserAdapter) StopSync() error {
	return nil
}

// GetIncrementalData 读取 lastSyncTime 之后变化的历史记录行。
func (a *BrowserAdapter) GetIncrementalData(lastSyncTime int64) ([]model.DataItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	items := make([]model.DataItem, 0, 512)
	for _, historyPath := range a.historyPaths() {
		next, err := a.readHistory(ctx, historyPath, lastSyncTime)
		if err != nil {
			continue
		}
		items = append(items, next...)
	}
	for _, bookmarkPath := range a.bookmarkPaths() {
		next, err := a.readBookmarks(bookmarkPath, lastSyncTime)
		if err == nil {
			items = append(items, next...)
		}
	}
	return items, nil
}

// readHistory 复制被锁定的浏览器数据库，并提取本地记录行。
func (a *BrowserAdapter) readHistory(
	ctx context.Context,
	historyPath string,
	lastSyncTime int64,
) ([]model.DataItem, error) {
	tempPath, cleanup, err := copyLockedDB(historyPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	db, err := sql.Open("sqlite", tempPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if a.Kind == BrowserFirefox {
		return a.readFirefox(ctx, db, lastSyncTime)
	}
	history, err := a.readChromium(ctx, db, lastSyncTime)
	if err != nil {
		return nil, err
	}
	downloads, err := a.readChromiumDownloads(ctx, db, lastSyncTime)
	if err == nil {
		history = append(history, downloads...)
	}
	return history, nil
}

// readChromium 从复制出的 History 数据库读取 Chromium URL 历史。
func (a *BrowserAdapter) readChromium(
	ctx context.Context,
	db *sql.DB,
	lastSyncTime int64,
) ([]model.DataItem, error) {
	rows, err := db.QueryContext(ctx, `
SELECT url, title, last_visit_time, visit_count
FROM urls
WHERE last_visit_time > ?
ORDER BY last_visit_time DESC
LIMIT 5000`, unixToChromeTime(lastSyncTime))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.DataItem
	for rows.Next() {
		item, err := scanChromiumRow(rows, string(a.Kind))
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// readFirefox 从 places.sqlite 读取 Firefox 历史和书签。
func (a *BrowserAdapter) readFirefox(
	ctx context.Context,
	db *sql.DB,
	lastSyncTime int64,
) ([]model.DataItem, error) {
	rows, err := db.QueryContext(ctx, `
SELECT url, title, last_visit_date, visit_count
FROM moz_places
WHERE last_visit_date > ?
ORDER BY last_visit_date DESC
LIMIT 5000`, lastSyncTime*1_000_000)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.DataItem
	for rows.Next() {
		item, err := scanFirefoxRow(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	bookmarks, err := a.readFirefoxBookmarks(ctx, db, lastSyncTime)
	if err == nil {
		items = append(items, bookmarks...)
	}
	return items, nil
}

// readChromiumDownloads 读取 Chromium 下载元数据。
func (a *BrowserAdapter) readChromiumDownloads(
	ctx context.Context,
	db *sql.DB,
	lastSyncTime int64,
) ([]model.DataItem, error) {
	rows, err := db.QueryContext(ctx, `
SELECT target_path, tab_url, start_time, received_bytes, total_bytes
FROM downloads
WHERE start_time > ?
ORDER BY start_time DESC
LIMIT 2000`, unixToChromeTime(lastSyncTime))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.DataItem
	for rows.Next() {
		item, err := scanChromiumDownload(rows, string(a.Kind))
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// readFirefoxBookmarks 读取 Firefox 书签行。
func (a *BrowserAdapter) readFirefoxBookmarks(
	ctx context.Context,
	db *sql.DB,
	lastSyncTime int64,
) ([]model.DataItem, error) {
	rows, err := db.QueryContext(ctx, `
SELECT p.url, COALESCE(b.title, p.title, ''), b.dateAdded
FROM moz_bookmarks b
JOIN moz_places p ON p.id = b.fk
WHERE b.type = 1 AND b.dateAdded > ?
ORDER BY b.dateAdded DESC
LIMIT 5000`, lastSyncTime*1_000_000)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.DataItem
	for rows.Next() {
		item, err := scanFirefoxBookmark(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// scanChromiumRow 将一行 Chromium 历史转换为 DataItem。
func scanChromiumRow(rows *sql.Rows, source string) (model.DataItem, error) {
	var url, title string
	var chromeTime, visits int64
	if err := rows.Scan(&url, &title, &chromeTime, &visits); err != nil {
		return model.DataItem{}, err
	}
	updated := chromeTimeToUnix(chromeTime)
	return browserItem(source, url, title, visits, updated), nil
}

// scanFirefoxRow 将一行 Firefox 历史转换为 DataItem。
func scanFirefoxRow(rows *sql.Rows) (model.DataItem, error) {
	var url string
	var title sql.NullString
	var visitDate sql.NullInt64
	var visits sql.NullInt64
	if err := rows.Scan(&url, &title, &visitDate, &visits); err != nil {
		return model.DataItem{}, err
	}
	updated := visitDate.Int64 / 1_000_000
	return browserItem("firefox", url, title.String, visits.Int64, updated), nil
}

// scanChromiumDownload 将一行 Chromium 下载转换为 DataItem。
func scanChromiumDownload(rows *sql.Rows, source string) (model.DataItem, error) {
	var targetPath, tabURL sql.NullString
	var chromeTime, receivedBytes, totalBytes int64
	if err := rows.Scan(&targetPath, &tabURL, &chromeTime, &receivedBytes, &totalBytes); err != nil {
		return model.DataItem{}, err
	}
	updated := chromeTimeToUnix(chromeTime)
	title := filepath.Base(targetPath.String)
	metadata := map[string]any{
		"path":           targetPath.String,
		"url":            tabURL.String,
		"received_bytes": receivedBytes,
		"total_bytes":    totalBytes,
		"kind":           "download",
	}
	return model.DataItem{
		ID:        "download:" + targetPath.String,
		Source:    source,
		Title:     title,
		Content:   title + "\n" + targetPath.String + "\n" + tabURL.String,
		Preview:   targetPath.String,
		Metadata:  metadata,
		CreatedAt: updated,
		UpdatedAt: updated,
	}, nil
}

// scanFirefoxBookmark 将一行 Firefox 书签转换为 DataItem。
func scanFirefoxBookmark(rows *sql.Rows) (model.DataItem, error) {
	var url, title string
	var added int64
	if err := rows.Scan(&url, &title, &added); err != nil {
		return model.DataItem{}, err
	}
	return bookmarkItem("firefox", url, title, added/1_000_000), nil
}

// browserItem 构造标准化历史条目。
func browserItem(source string, url string, title string, visits int64, updated int64) model.DataItem {
	if strings.TrimSpace(title) == "" {
		title = url
	}
	metadata := map[string]any{
		"url":         url,
		"visit_count": visits,
	}
	return model.DataItem{
		ID:        url,
		Source:    source,
		Title:     title,
		Content:   title + "\n" + url,
		Preview:   url,
		Metadata:  metadata,
		CreatedAt: updated,
		UpdatedAt: updated,
	}
}

// bookmarkItem 构造标准化书签条目。
func bookmarkItem(source string, url string, title string, updated int64) model.DataItem {
	if strings.TrimSpace(title) == "" {
		title = url
	}
	metadata := map[string]any{"url": url, "kind": "bookmark"}
	return model.DataItem{
		ID:        "bookmark:" + url,
		Source:    source,
		Title:     title,
		Content:   title + "\n" + url,
		Preview:   url,
		Metadata:  metadata,
		CreatedAt: updated,
		UpdatedAt: updated,
	}
}

// historyPaths 返回浏览器历史数据库位置。
func (a *BrowserAdapter) historyPaths() []string {
	if runtime.GOOS != "windows" {
		return nil
	}
	local := os.Getenv("LOCALAPPDATA")
	roaming := os.Getenv("APPDATA")
	switch a.Kind {
	case BrowserChrome:
		return []string{filepath.Join(local, "Google", "Chrome", "User Data", "Default", "History")}
	case BrowserEdge:
		return []string{filepath.Join(local, "Microsoft", "Edge", "User Data", "Default", "History")}
	case BrowserFirefox:
		return firefoxProfiles(roaming)
	default:
		return nil
	}
}

// bookmarkPaths 返回 Chromium 书签 JSON 位置。
func (a *BrowserAdapter) bookmarkPaths() []string {
	if runtime.GOOS != "windows" {
		return nil
	}
	local := os.Getenv("LOCALAPPDATA")
	switch a.Kind {
	case BrowserChrome:
		return []string{filepath.Join(local, "Google", "Chrome", "User Data", "Default", "Bookmarks")}
	case BrowserEdge:
		return []string{filepath.Join(local, "Microsoft", "Edge", "User Data", "Default", "Bookmarks")}
	default:
		return nil
	}
}

// firefoxProfiles 返回 Firefox 配置数据库位置。
func firefoxProfiles(roaming string) []string {
	root := filepath.Join(roaming, "Mozilla", "Firefox", "Profiles")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			paths = append(paths, filepath.Join(root, entry.Name(), "places.sqlite"))
		}
	}
	return paths
}

// copyLockedDB 将正在使用的浏览器数据库复制为临时只读文件。
func copyLockedDB(path string) (string, func(), error) {
	source, err := os.Open(path)
	if err != nil {
		return "", func() {}, err
	}
	defer source.Close()

	target, err := os.CreateTemp("", "recall-browser-*.sqlite")
	if err != nil {
		return "", func() {}, err
	}
	defer target.Close()

	if _, err := io.Copy(target, source); err != nil {
		_ = os.Remove(target.Name())
		return "", func() {}, err
	}
	return target.Name(), func() { _ = os.Remove(target.Name()) }, nil
}

// chromeTimeToUnix 将 Chromium WebKit 时间戳转换为 Unix 秒。
func chromeTimeToUnix(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return (value / 1_000_000) - 11644473600
}

// unixToChromeTime 将 Unix 秒转换为 Chromium WebKit 时间戳。
func unixToChromeTime(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return (value + 11644473600) * 1_000_000
}

// BrowserBookmark 是用于书签索引的标准化书签节点。
type BrowserBookmark struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Date int64  `json:"date"`
}

// readBookmarks 读取 Chromium 书签 JSON 文件。
func (a *BrowserAdapter) readBookmarks(path string, lastSyncTime int64) ([]model.DataItem, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	bookmarks, err := decodeBookmarks(data)
	if err != nil {
		return nil, err
	}

	items := make([]model.DataItem, 0, len(bookmarks))
	for _, bookmark := range bookmarks {
		updated := chromeTimeToUnix(bookmark.Date)
		if updated > lastSyncTime {
			items = append(items, bookmarkItem(string(a.Kind), bookmark.URL, bookmark.Name, updated))
		}
	}
	return items, nil
}

// decodeBookmarks 展平 Chromium 书签 JSON 根节点。
func decodeBookmarks(data []byte) ([]BrowserBookmark, error) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	roots, ok := raw["roots"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid bookmark file")
	}

	var bookmarks []BrowserBookmark
	for _, root := range roots {
		collectBookmarks(root, &bookmarks)
	}
	return bookmarks, nil
}

// collectBookmarks 递归访问 Chromium 书签节点。
func collectBookmarks(node any, bookmarks *[]BrowserBookmark) {
	value, ok := node.(map[string]any)
	if !ok {
		return
	}
	if value["type"] == "url" {
		date, _ := parseChromeTimestamp(value["date_added"])
		name, _ := value["name"].(string)
		url, _ := value["url"].(string)
		if url != "" {
			*bookmarks = append(*bookmarks, BrowserBookmark{Name: name, URL: url, Date: date})
		}
	}
	children, _ := value["children"].([]any)
	for _, child := range children {
		collectBookmarks(child, bookmarks)
	}
}

// parseChromeTimestamp 从 JSON 解析 Chromium 时间戳。
func parseChromeTimestamp(value any) (int64, bool) {
	switch typed := value.(type) {
	case string:
		var parsed int64
		if _, err := fmt.Sscan(typed, &parsed); err == nil {
			return parsed, true
		}
	case float64:
		return int64(typed), true
	}
	return 0, false
}

// titleCaseASCII 将第一个 ASCII 字符转为大写。
func titleCaseASCII(value string) string {
	if value == "" {
		return value
	}
	return strings.ToUpper(value[:1]) + value[1:]
}
