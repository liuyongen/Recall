package indexer

import (
	"strings"
	"unicode"
)

// BuildFTSQuery turns user input into an FTS5 MATCH expression.
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

// looksAdvanced detects explicit FTS5 syntax supplied by the user.
func looksAdvanced(input string) bool {
	upper := strings.ToUpper(input)
	return strings.ContainsAny(input, "\"*()") ||
		strings.Contains(upper, " AND ") ||
		strings.Contains(upper, " OR ") ||
		strings.Contains(upper, " NOT ") ||
		strings.Contains(upper, "NEAR")
}

// tokenizeSimple extracts conservative terms from plain search text.
func tokenizeSimple(input string) []string {
	return strings.FieldsFunc(input, func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' || r > unicode.MaxASCII)
	})
}

// isBareToken reports whether a token can safely use FTS prefix syntax.
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

// quoteFTS escapes a token as an FTS5 phrase.
func quoteFTS(token string) string {
	return `"` + strings.ReplaceAll(token, `"`, `""`) + `"`
}
