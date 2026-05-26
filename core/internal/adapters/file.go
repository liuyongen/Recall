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

	"phantasm/core/internal/extract"
	"phantasm/core/internal/model"
)

// FileAdapter indexes user-selected local files and directories.
type FileAdapter struct {
	Roots     []string
	Extractor extract.Extractor
	MaxBytes  int64
}

// FileCandidate is a cheap stat-derived file candidate for indexing.
type FileCandidate struct {
	Path      string
	Size      int64
	ModTimeNS int64
	IsDir     bool
}

// CandidateFilter decides whether a candidate is already known unchanged.
type CandidateFilter func(FileCandidate) (skip bool, known bool)

// NewFileAdapter creates a file adapter for the provided roots.
func NewFileAdapter(roots []string, extractor extract.Extractor, maxBytes int64) *FileAdapter {
	if maxBytes <= 0 {
		maxBytes = extract.DefaultMaxBytes
	}
	return &FileAdapter{Roots: roots, Extractor: extractor, MaxBytes: maxBytes}
}

// ID returns the stable adapter identifier.
func (a *FileAdapter) ID() string {
	return "file"
}

// Name returns a human-readable adapter name.
func (a *FileAdapter) Name() string {
	return "Local Files"
}

// IsAvailable reports whether at least one configured root exists.
func (a *FileAdapter) IsAvailable() bool {
	for _, root := range a.Roots {
		if _, err := os.Stat(root); err == nil {
			return true
		}
	}
	return false
}

// GetIncrementalData extracts files changed after the supplied Unix timestamp.
func (a *FileAdapter) GetIncrementalData(lastSyncTime int64) ([]model.DataItem, error) {
	items := make([]model.DataItem, 0, 256)
	err := a.WalkIncrementalData(context.Background(), lastSyncTime, func(item model.DataItem) error {
		items = append(items, item)
		return nil
	})
	return items, err
}

// WalkIncrementalData streams changed file items to a visitor.
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

// WalkIncrementalPaths streams candidate file paths changed after lastSyncTime.
func (a *FileAdapter) WalkIncrementalPaths(
	ctx context.Context,
	lastSyncTime int64,
	visit func(string) error,
) error {
	return a.WalkIncrementalCandidates(ctx, lastSyncTime, nil, func(candidate FileCandidate) error {
		return visit(candidate.Path)
	})
}

// WalkIncrementalCandidates streams candidate files with stat metadata.
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

// walkRoot visits a file or directory tree and appends changed items.
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
			info, err := d.Info()
			if err != nil {
				return nil
			}
			candidate := FileCandidate{
				Path:      path,
				Size:      info.Size(),
				ModTimeNS: info.ModTime().UnixNano(),
				IsDir:     true,
			}
			if filter != nil {
				if skip, known := filter(candidate); skip {
					return nil
				} else if known {
					return visit(candidate)
				}
			}
			if info.ModTime().Unix() <= lastSyncTime {
				return nil
			}
			return visit(candidate)
		}

		if shouldSkipFilePath(path) {
			return nil
		}
		if shouldSkipFileName(d.Name()) {
			return nil
		}
		if !extract.SupportsIndexedPath(path) {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.IsDir() || info.Size() > a.MaxBytes {
			return nil
		}
		candidate := FileCandidate{
			Path:      path,
			Size:      info.Size(),
			ModTimeNS: info.ModTime().UnixNano(),
			IsDir:     false,
		}
		if filter != nil {
			if skip, known := filter(candidate); skip {
				return nil
			} else if known {
				return visit(candidate)
			}
		}
		if info.ModTime().Unix() <= lastSyncTime {
			return nil
		}
		return visit(candidate)
	})
}

// extractFile extracts one changed file into a DataItem.
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

// ShouldSkipDir filters noisy directories that should not be indexed.
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
	if _, ok := skippedDirNames[name]; ok {
		return true
	}
	if isNoisyFilePath(path) {
		return true
	}
	return runtime.GOOS == "windows" && isWindowsSystemDir(path)
}

// shouldSkipFilePath filters generated files inside known noisy directories.
func shouldSkipFilePath(path string) bool {
	return isNoisyFilePath(path)
}

// shouldSkipFileName filters hidden, temp, and known-noise file names.
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

// isNoisyFilePath detects app bundles and caches that drown useful documents.
func isNoisyFilePath(path string) bool {
	normalized := strings.ToLower(filepath.ToSlash(filepath.Clean(path)))
	for _, fragment := range skippedPathFragments {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return false
}

// StableFileID creates a stable local identifier from the absolute path.
func StableFileID(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	normalized := strings.ToLower(filepath.Clean(absolute))
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
}

// DefaultFileRoots returns the user's standard personal folders.
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

// String returns a compact diagnostic description.
func (a *FileAdapter) String() string {
	return fmt.Sprintf("file roots=%d", len(a.Roots))
}

var skippedDirNames = map[string]struct{}{
	"$recycle.bin":              {},
	".idea":                     {},
	".vscode":                   {},
	"__pycache__":               {},
	"appdata":                   {},
	"bin":                       {},
	"build":                     {},
	"cache":                     {},
	"dist":                      {},
	"dist-electron":             {},
	"logs":                      {},
	"node_modules":              {},
	"obj":                       {},
	"out":                       {},
	"target":                    {},
	"temp":                      {},
	"tmp":                       {},
	"vendor":                    {},
	"venv":                      {},
	"windows.old":               {},
	"system volume information": {},
}

var skippedFileNames = map[string]struct{}{
	".ds_store":  {},
	"desktop.ini": {},
	"thumbs.db":  {},
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

// ExtractFile extracts a single file into a DataItem; ok=false if skipped.
func (a *FileAdapter) ExtractFile(ctx context.Context, path string) (model.DataItem, bool) {
	return a.extractFile(ctx, path, 0)
}

// ExtractChangedFile extracts a file if it is still changed relative to lastSyncTime.
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
	"/gpucache/",
	"/jssdk/",
	"/pip/cache/",
	"/software/dingding/",
	"/software/dingtalk/",
	"/software/feishu/",
	"/software/lark/",
	"/web_content/",
	"/webcontent/",
}

// isWindowsSystemDir skips high-noise and permission-heavy Windows roots.
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
