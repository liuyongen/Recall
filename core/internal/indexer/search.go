package indexer

import (
	"strings"
	"unicode"
)

// BuildFTSQuery 将用户输入转换为 FTS5 MATCH 表达式。
func BuildFTSQuery(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	if looksAdvanced(input) {
		return input
	}

	tokens := tokenizeSimple(input)
	terms := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if isBareToken(token) {
			terms = append(terms, token+"*")
			continue
		}
		terms = append(terms, quoteFTS(token))
	}
	return strings.Join(terms, " AND ")
}

// looksAdvanced 检测用户是否显式提供了 FTS5 语法。
func looksAdvanced(input string) bool {
	upper := strings.ToUpper(input)
	return strings.ContainsAny(input, "\"*()") ||
		strings.Contains(upper, " AND ") ||
		strings.Contains(upper, " OR ") ||
		strings.Contains(upper, " NOT ") ||
		strings.Contains(upper, "NEAR")
}

// tokenizeSimple 从普通搜索文本中提取保守的搜索词。
func tokenizeSimple(input string) []string {
	return strings.FieldsFunc(input, func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' || r > unicode.MaxASCII)
	})
}

// isBareToken 判断词元是否可以安全使用 FTS 前缀语法。
func isBareToken(token string) bool {
	if token == "" {
		return false
	}
	for _, r := range token {
		if !(r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)) || r > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// quoteFTS 将词元转义为 FTS5 短语。
func quoteFTS(token string) string {
	return `"` + strings.ReplaceAll(token, `"`, `""`) + `"`
}
