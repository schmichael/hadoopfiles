package hadoopfiles

import (
	"strconv"
	"unicode/utf8"
)

const lowerhex = "0123456789abcdef"

// Returns an escaped version of rune. Escaping letters that produce control
// codes (n => \n) will produce undesirable results.
//
// Modified version of strconv/quote.go:quoteWith
func escape(r rune) string {
	if r == utf8.RuneError {
		return `\uFFFD`
	}
	if strconv.IsPrint(r) {
		// Printable characters just a get a backslash prepended. Lowercase ascii characters that are used when escaping control codes 
		return `\` + string(r)
	}
	switch r {
	case '\a':
		return `\a`
	case '\b':
		return `\b`
	case '\f':
		return `\f`
	case '\n':
		return `\n`
	case '\r':
		return `\r`
	case '\t':
		return `\t`
	case '\v':
		return `\v`
	default:
		switch {
		case r < ' ':
			buf := make([]byte, 0, 4)
			buf = append(buf, `\x`...)
			buf = append(buf, lowerhex[r>>4])
			buf = append(buf, lowerhex[r&0xF])
			return string(buf)
		case r > utf8.MaxRune:
			return string(utf8.RuneError)
		case r < 0x10000:
			buf := make([]byte, 0, 6)
			buf = append(buf, `\u`...)
			for s := 12; s >= 0; s -= 4 {
				buf = append(buf, lowerhex[r>>uint(s)&0xF])
			}
			return string(buf)
		default:
			buf := make([]byte, 0, 10)
			buf = append(buf, `\U`...)
			for s := 28; s >= 0; s -= 4 {
				buf = append(buf, lowerhex[r>>uint(s)&0xF])
			}
			return string(buf)
		}
	}
}
