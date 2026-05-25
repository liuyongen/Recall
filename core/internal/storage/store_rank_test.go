package storage

import (
	"testing"
	"time"

	"phantasm/core/internal/model"
)

func TestRerankPrefersExactFilename(t *testing.T) {
	now := time.Now().Unix()
	results := []model.SearchResult{
		{
			Source:    "file",
			Title:     "meeting notes",
			Preview:   "contains report final references",
			Path:      `C:/Users/test/Documents/notes/meeting.txt`,
			FileType:  "txt",
			UpdatedAt: now,
			Score:     0,
		},
		{
			Source:    "file",
			Title:     "report-final.docx",
			Preview:   "",
			Path:      `C:/Users/test/Documents/reports/report-final.docx`,
			FileType:  "docx",
			UpdatedAt: now,
			Score:     0,
		},
	}

	ranked := rerankSearchResults(results, "report final", 10)
	if len(ranked) < 2 {
		t.Fatalf("expected at least 2 ranked results, got %d", len(ranked))
	}
	if ranked[0].Path != `c:/users/test/documents/reports/report-final.docx` && ranked[0].Path != `C:/Users/test/Documents/reports/report-final.docx` {
		t.Fatalf("expected exact filename hit first, got %q", ranked[0].Path)
	}
}

func TestRerankDemotesNoisyGeneratedAssets(t *testing.T) {
	now := time.Now().Unix()
	results := []model.SearchResult{
		{
			Source:    "file",
			Title:     "react",
			Preview:   "",
			Path:      `C:/project/node_modules/react/dist/react.min.js`,
			FileType:  "js",
			UpdatedAt: now,
			Score:     0,
		},
		{
			Source:    "file",
			Title:     "React guide",
			Preview:   "developer notes",
			Path:      `C:/Users/test/Documents/react-guide.md`,
			FileType:  "md",
			UpdatedAt: now,
			Score:     0,
		},
	}

	ranked := rerankSearchResults(results, "react", 10)
	if len(ranked) < 1 {
		t.Fatalf("expected at least 1 ranked result, got %d", len(ranked))
	}
	if ranked[0].Path != `C:/Users/test/Documents/react-guide.md` {
		t.Fatalf("expected personal document first, got %q", ranked[0].Path)
	}
	for _, result := range ranked {
		if result.Path == `C:/project/node_modules/react/dist/react.min.js` {
			t.Fatalf("expected noisy generated asset to be filtered or strongly demoted")
		}
	}
}

func TestRerankPrefersFolderForFolderIntent(t *testing.T) {
	now := time.Now().Unix()
	results := []model.SearchResult{
		{
			Source:    "file",
			Title:     "Design",
			Preview:   "folder: C:/Users/test/Documents/Design",
			Path:      `C:/Users/test/Documents/Design`,
			FileType:  "folder",
			UpdatedAt: now,
			Score:     0,
		},
		{
			Source:    "file",
			Title:     "design-spec.md",
			Preview:   "",
			Path:      `C:/Users/test/Documents/Design/design-spec.md`,
			FileType:  "md",
			UpdatedAt: now,
			Score:     0,
		},
	}

	ranked := rerankSearchResults(results, "design folder", 10)
	if len(ranked) < 2 {
		t.Fatalf("expected at least 2 ranked results, got %d", len(ranked))
	}
	if ranked[0].FileType != "folder" {
		t.Fatalf("expected folder result first for folder intent, got file_type=%q path=%q", ranked[0].FileType, ranked[0].Path)
	}
}
