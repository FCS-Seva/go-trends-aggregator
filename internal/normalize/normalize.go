package normalize

import (
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const MaxQueryRunes = 256

func Query(s string) string {
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, "ё", "е")
	if utf8.RuneCountInString(s) > MaxQueryRunes {
		runes := []rune(s)
		s = string(runes[:MaxQueryRunes])
	}
	return s
}

func StopTerm(s string) string {
	return Query(s)
}
