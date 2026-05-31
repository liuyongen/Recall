package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"recall/core/internal/extract"
	"recall/core/internal/model"
)

// FileAdapter 索引用户选择的本地文件和目录。
type FileAdapter struct {
	Roots       []string
	Extractor   extract.Extractor
	MaxBytes    int64
	PathOnly    bool
	ContentOnly bool
}

// FileCandidate 是基于 stat 信息得到的轻量索引候选文件。
type FileCandidate struct {
	Path      string
	Size      int64
	ModTimeNS int64
	IsDir     bool
}

// CandidateFilter 判断候选文件是否已知未变化。
type CandidateFilter func(FileCandidate) (skip bool, known bool)

// NewFileAdapter 为给定根路径创建文件适配器。
func NewFileAdapter(roots []string, extractor extract.Extractor, maxBytes int64) *FileAdapter {
	if maxBytes <= 0 {
		maxBytes = extract.DefaultMaxBytes
	}
	return &FileAdapter{Roots: roots, Extractor: extractor, MaxBytes: maxBytes}
}

// ID 返回稳定的适配器标识。
func (a *FileAdapter) ID() string {
	return "file"
}

// Name 返回适合用户阅读的适配器名称。
func (a *FileAdapter) Name() string {
	return "Local Files"
}

// IsAvailable 判断是否至少有一个配置的根路径存在。
func (a *FileAdapter) IsAvailable() bool {
	for _, root := range a.Roots {
		if _, err := os.Stat(root); err == nil {
			return true
		}
	}
	return false
}

// GetIncrementalData 提取给定 Unix 时间戳之后变化的文件。
func (a *FileAdapter) GetIncrementalData(lastSyncTime int64) ([]model.DataItem, error) {
	items := make([]model.DataItem, 0, 256)
	err := a.WalkIncrementalData(context.Background(), lastSyncTime, func(item model.DataItem) error {
		items = append(items, item)
		return nil
	})
	return items, err
}

// WalkIncrementalData 将变化的文件条目流式传给 visitor。
func (a *FileAdapter) WalkIncrementalData(
	ctx context.Context,
	lastSyncTime int64,
	visit func(model.DataItem) error,
) error {
	for _, root := range a.Roots {
		if err := a.walkRoot(ctx, root, lastSyncTime, visit); err != nil {
			return err
		}
	}
	return nil
}

// WalkIncrementalPaths 流式产出 lastSyncTime 之后变化的候选文件路径。
func (a *FileAdapter) WalkIncrementalPaths(
	ctx context.Context,
	lastSyncTime int64,
	visit func(string) error,
) error {
	return a.WalkIncrementalCandidates(ctx, lastSyncTime, nil, func(candidate FileCandidate) error {
		return visit(candidate.Path)
	})
}

// WalkIncrementalCandidates 流式产出带 stat 元数据的候选文件。
func (a *FileAdapter) WalkIncrementalCandidates(
	ctx context.Context,
	lastSyncTime int64,
	filter CandidateFilter,
	visit func(FileCandidate) error,
) error {
	for _, root := range a.Roots {
		if err := a.walkRootCandidates(ctx, root, lastSyncTime, filter, visit); err != nil {
			return err
		}
	}
	return nil
}

// walkRoot 遍历文件或目录树，并追加变化条目。
func (a *FileAdapter) walkRoot(
	ctx context.Context,
	root string,
	lastSyncTime int64,
	visit func(model.DataItem) error,
) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if ShouldSkipDir(path, d) {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		if shouldSkipFileName(d.Name()) {
			return nil
		}
		item, ok := a.extractFile(ctx, path, lastSyncTime)
		if ok {
			return visit(item)
		}
		return nil
	})
}

func (a *FileAdapter) walkRootCandidates(
	ctx context.Context,
	root string,
	lastSyncTime int64,
	filter CandidateFilter,
	visit func(FileCandidate) error,
) error {
	info, err := os.Lstat(root)
	if err != nil {
		return nil
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	if !info.IsDir() {
		return a.visitFileCandidate(ctx, root, info, lastSyncTime, filter, visit)
	}

	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	queue := newDirQueue()
	queue.push(root)

	var (
		errMu    sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
			queue.wake()
		}
		errMu.Unlock()
	}

	workers := fileWalkWorkers()
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				dir, ok := queue.pop(walkCtx)
				if !ok {
					return
				}
				a.scanCandidateDir(walkCtx, dir, lastSyncTime, filter, visit, queue, setErr)
				queue.done()
			}
		}()
	}
	wg.Wait()

	errMu.Lock()
	defer errMu.Unlock()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func (a *FileAdapter) scanCandidateDir(
	ctx context.Context,
	dir string,
	lastSyncTime int64,
	filter CandidateFilter,
	visit func(FileCandidate) error,
	queue *dirQueue,
	setErr func(error),
) {
	if err := ctx.Err(); err != nil {
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return
		}
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if entry.IsDir() {
			if ShouldSkipDir(path, entry) {
				continue
			}
			info, err := entry.Info()
			if err == nil {
				if err := visitCandidate(ctx, FileCandidate{
					Path:      path,
					Size:      info.Size(),
					ModTimeNS: info.ModTime().UnixNano(),
					IsDir:     true,
				}, lastSyncTime, filter, visit); err != nil {
					setErr(err)
					return
				}
			}
			queue.push(path)
			continue
		}

		if shouldSkipFilePath(path) || shouldSkipFileName(entry.Name()) || !extract.SupportsIndexedPath(path) {
			continue
		}
		info, err := entry.Info()
		if err != nil || info.IsDir() || info.Size() > a.MaxBytes {
			continue
		}
		if err := visitCandidate(ctx, FileCandidate{
			Path:      path,
			Size:      info.Size(),
			ModTimeNS: info.ModTime().UnixNano(),
			IsDir:     false,
		}, lastSyncTime, filter, visit); err != nil {
			setErr(err)
			return
		}
	}
}

func (a *FileAdapter) visitFileCandidate(
	ctx context.Context,
	path string,
	info os.FileInfo,
	lastSyncTime int64,
	filter CandidateFilter,
	visit func(FileCandidate) error,
) error {
	if shouldSkipFilePath(path) || shouldSkipFileName(filepath.Base(path)) || !extract.SupportsIndexedPath(path) {
		return nil
	}
	if info.Size() > a.MaxBytes {
		return nil
	}
	return visitCandidate(ctx, FileCandidate{
		Path:      path,
		Size:      info.Size(),
		ModTimeNS: info.ModTime().UnixNano(),
		IsDir:     false,
	}, lastSyncTime, filter, visit)
}

func visitCandidate(
	ctx context.Context,
	candidate FileCandidate,
	lastSyncTime int64,
	filter CandidateFilter,
	visit func(FileCandidate) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if filter != nil {
		if skip, known := filter(candidate); skip {
			return nil
		} else if known {
			return visit(candidate)
		}
	}
	if timeFromNS(candidate.ModTimeNS) <= lastSyncTime {
		return nil
	}
	return visit(candidate)
}

func timeFromNS(ns int64) int64 {
	return ns / int64(time.Second)
}

func fileWalkWorkers() int {
	workers := runtime.NumCPU() * 2
	if workers < 4 {
		return 4
	}
	if workers > 32 {
		return 32
	}
	return workers
}

type dirQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	dirs    []string
	head    int
	active  int
	stopped bool
}

func newDirQueue() *dirQueue {
	q := &dirQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *dirQueue) push(path string) {
	q.mu.Lock()
	if !q.stopped {
		q.dirs = append(q.dirs, path)
		q.cond.Signal()
	}
	q.mu.Unlock()
}

func (q *dirQueue) pop(ctx context.Context) (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.head >= len(q.dirs) && q.active > 0 && !q.stopped && ctx.Err() == nil {
		q.cond.Wait()
	}
	if q.stopped || ctx.Err() != nil || q.head >= len(q.dirs) {
		q.stopped = true
		q.cond.Broadcast()
		return "", false
	}
	dir := q.dirs[q.head]
	q.head++
	if q.head > 4096 && q.head*2 > len(q.dirs) {
		copy(q.dirs, q.dirs[q.head:])
		q.dirs = q.dirs[:len(q.dirs)-q.head]
		q.head = 0
	}
	q.active++
	return dir, true
}

func (q *dirQueue) done() {
	q.mu.Lock()
	q.active--
	if q.head >= len(q.dirs) && q.active == 0 {
		q.stopped = true
		q.cond.Broadcast()
	} else {
		q.cond.Signal()
	}
	q.mu.Unlock()
}

func (q *dirQueue) wake() {
	q.mu.Lock()
	q.stopped = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

// extractFile 将一个变化文件提取为 DataItem。
func (a *FileAdapter) extractFile(ctx context.Context, path string, lastSyncTime int64) (model.DataItem, bool) {
	if shouldSkipFilePath(path) {
		return model.DataItem{}, false
	}
	if shouldSkipFileName(filepath.Base(path)) {
		return model.DataItem{}, false
	}
	if !extract.SupportsIndexedPath(path) {
		return model.DataItem{}, false
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() || info.Size() > a.MaxBytes {
		return model.DataItem{}, false
	}
	if info.ModTime().Unix() <= lastSyncTime {
		return model.DataItem{}, false
	}

	text, metadata, err := a.Extractor.Extract(ctx, path)
	if err != nil || strings.TrimSpace(text) == "" {
		return model.DataItem{}, false
	}

	metadata["path"] = path
	metadata["file_type"] = strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
	return model.DataItem{
		ID:        StableFileID(path),
		Source:    "file",
		Title:     filepath.Base(path),
		Content:   text,
		Preview:   "",
		Metadata:  metadata,
		CreatedAt: info.ModTime().Unix(),
		UpdatedAt: info.ModTime().Unix(),
	}, true
}

// ShouldSkipDir 过滤不应索引的高噪声目录。
func ShouldSkipDir(path string, entry os.DirEntry) bool {
	if entry == nil || !entry.IsDir() {
		return false
	}
	if entry.Type()&os.ModeSymlink != 0 {
		return true
	}
	name := strings.ToLower(entry.Name())
	if strings.HasPrefix(name, ".") {
		return true
	}
	if strings.HasPrefix(name, "skin_") {
		return true
	}
	if _, ok := skippedDirNames[name]; ok {
		return true
	}
	if isNoisyFilePath(path) {
		return true
	}
	return runtime.GOOS == "windows" && isWindowsSystemDir(path)
}

// shouldSkipFilePath 过滤已知高噪声目录中的生成文件。
func shouldSkipFilePath(path string) bool {
	return isNoisyFilePath(path)
}

// shouldSkipFileName 过滤隐藏、临时和已知噪声文件名。
func shouldSkipFileName(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if lower == "" {
		return true
	}
	if strings.HasPrefix(lower, ".") || strings.HasPrefix(lower, "~$") {
		return true
	}
	if _, ok := skippedFileNames[lower]; ok {
		return true
	}
	for _, suffix := range skippedFileSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return false
}

// isNoisyFilePath 检测会淹没有用文档的应用包和缓存路径。
func isNoisyFilePath(path string) bool {
	normalized := strings.ToLower(filepath.ToSlash(filepath.Clean(path)))
	for _, fragment := range skippedPathFragments {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return false
}

// StableFileID 根据绝对路径创建稳定的本地标识。
func StableFileID(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	normalized := strings.ToLower(filepath.Clean(absolute))
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
}

// DefaultFileRoots 返回用户的标准个人文件夹。
func DefaultFileRoots() []string {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	candidates := []string{
		filepath.Join(home, "Desktop"),
		filepath.Join(home, "Documents"),
		filepath.Join(home, "Downloads"),
	}
	roots := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			roots = append(roots, candidate)
		}
	}
	return roots
}

// String 返回紧凑的诊断描述。
func (a *FileAdapter) String() string {
	return fmt.Sprintf("file roots=%d", len(a.Roots))
}

var skippedDirNames = map[string]struct{}{
	"$recycle.bin":              {},
	".idea":                     {},
	".vscode":                   {},
	"__pycache__":               {},
	"addons":                    {},
	"assets":                    {},
	"appdata":                   {},
	"bin":                       {},
	"build":                     {},
	"cache":                     {},
	"dist":                      {},
	"dist-electron":             {},
	"doc":                       {},
	"docs":                      {},
	"documentation":             {},
	"example":                   {},
	"examples":                  {},
	"font":                      {},
	"fonts":                     {},
	"image":                     {},
	"images":                    {},
	"img":                       {},
	"imgs":                      {},
	"logs":                      {},
	"mui":                       {},
	"node_modules":              {},
	"obj":                       {},
	"office6":                   {},
	"out":                       {},
	"plugin":                    {},
	"plugins":                   {},
	"res":                       {},
	"resource":                  {},
	"resources":                 {},
	"sample":                    {},
	"samples":                   {},
	"skin":                      {},
	"skins":                     {},
	"static":                    {},
	"target":                    {},
	"test":                      {},
	"testdata":                  {},
	"tests":                     {},
	"temp":                      {},
	"tmp":                       {},
	"uxkit":                     {},
	"vendor":                    {},
	"venv":                      {},
	"windows.old":               {},
	"system volume information": {},
}

var skippedFileNames = map[string]struct{}{
	".ds_store":   {},
	"desktop.ini": {},
	"thumbs.db":   {},
}

var skippedFileSuffixes = []string{
	".bak",
	".cache",
	".crdownload",
	".download",
	".lock",
	".partial",
	".part",
	".swp",
	".swo",
	".temp",
	".tmp",
	"~",
}

// ExtractFile 将单个文件提取为 DataItem；如果被跳过则 ok=false。
func (a *FileAdapter) ExtractFile(ctx context.Context, path string) (model.DataItem, bool) {
	return a.extractFile(ctx, path, 0)
}

// ExtractChangedFile 在文件相对 lastSyncTime 仍有变化时提取它。
func (a *FileAdapter) ExtractChangedFile(ctx context.Context, path string, lastSyncTime int64) (model.DataItem, bool) {
	return a.extractFile(ctx, path, lastSyncTime)
}

var skippedPathFragments = []string{
	"/appdata/local/google/chrome/user data/",
	"/appdata/local/microsoft/windows/inetcache/",
	"/appdata/local/microsoft/edge/user data/",
	"/appdata/roaming/code/cacheddata/",
	"/appdata/roaming/code/cache/",
	"/appdata/roaming/npm-cache/",
	"/code cache/",
	"/cocos2d-x-",
	"/gpucache/",
	"/jssdk/",
	"/pip/cache/",
	"/software/wps office/",
	"/visual studio packages/",
	"/software/dingding/",
	"/software/dingtalk/",
	"/software/feishu/",
	"/software/lark/",
	"/web_content/",
	"/webcontent/",
}

// isWindowsSystemDir 跳过高噪声且权限负担重的 Windows 根目录。
func isWindowsSystemDir(path string) bool {
	cleaned := strings.ToLower(filepath.Clean(path))
	volume := strings.ToLower(filepath.VolumeName(cleaned))
	if volume == "" {
		return false
	}
	relative := strings.TrimPrefix(cleaned, volume)
	relative = strings.TrimPrefix(relative, string(filepath.Separator))
	firstPart := relative
	if idx := strings.IndexRune(relative, filepath.Separator); idx >= 0 {
		firstPart = relative[:idx]
	}
	switch firstPart {
	case "windows", "program files", "program files (x86)", "programdata", "perflogs":
		return true
	default:
		return false
	}
}
