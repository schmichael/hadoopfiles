// Simple package for writing Hadoop/Hive formatted text files. Handles
// escaping and supports arbitrary delimiters.
package hadoopfiles

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	TimestampFormat = "2006-01-02 15:04:05.999999999"

	DefaultFieldDelimiter  = 1
	DefaultItemDelimiter   = 2
	DefaultMapKeyDelimiter = 3
	DefaultLineEnding      = '\n'
)

type RowWriter struct {
	buf             *bytes.Buffer
	fieldDelimiter  byte
	itemDelimiter   byte
	mapKeyDelimiter byte
	lineEnding      byte
	replacer        *strings.Replacer
	delims          string // used for checking non-UTF8 strings w/Contains
}

// Creates a new RowWriter with the default delimiters. Overwrite delimiters
// with SetDelimiters.
func NewRowWriter() *RowWriter {
	w := &RowWriter{buf: bytes.NewBuffer(nil)}
	err := w.SetDelimiters(
		DefaultFieldDelimiter,
		DefaultItemDelimiter,
		DefaultMapKeyDelimiter,
		DefaultLineEnding,
	)
	if err != nil {
		panic("Default delimiters are invalid: " + err.Error())
	}
	return w
}

// Sets the delimiters for a row.
//
// Delimiters must not have their high order bit set (be <128) and cannot be
// lowercase ASCII letters, digits, or U. These restrictions are to prevent
// ambiguous escape codes (escaping 'n' to "\n").
func (w *RowWriter) SetDelimiters(field, item, key, line byte) error {
	if w.buf.Len() > 0 {
		return fmt.Errorf("Cannot set delimiters after starting to write a row.")
	}
	names := []string{"field", "item", "key", "line"} // used in error message
	delims := []byte{field, item, key, line}
	pairs := make([]string, 0, (1+len(delims))*2)

	// Escape the escape character!
	pairs = append(pairs, `\`, `\\`)

	// Used for strings.Contains when checking non-UTF8 strings
	delimStr := string(field) + string(item) + string(key) + string(line)

	if field == item || field == key || field == line || item == key || item == line || key == line {
		return fmt.Errorf("Cannot have duplicate delimiters: %s", delimStr)
	}

	for i, d := range delims {
		if d > 127 || (d > 96 && d < 123) || (d > 47 && d < 58) || d == 'U' || d == '\\' {
			// High order bit set, lowercase ascii character, digits, or uppercase U:
			// cannot safely replace!
			return fmt.Errorf("%q is not a valid %s delimiter", d, names[i])
		}
		// Add original and escaped-replacement pair to list of pairs for replacer.
		pairs = append(pairs, string(d), escape(rune(d)))
	}
	w.delims = delimStr
	w.replacer = strings.NewReplacer(pairs...)
	w.fieldDelimiter = field
	w.itemDelimiter = item
	w.mapKeyDelimiter = key
	w.lineEnding = line
	return nil
}

// Writes a field or returns false if type isn't a supported.
func (w *RowWriter) WriteField(raw interface{}) bool {
	switch v := raw.(type) {
	case string:
		w.WriteString(v)
	case int:
		w.WriteInt(v)
	case int32, int64, uint, uint32, uint64:
		w.WriteString(fmt.Sprintf("%d", v))
	case float32, float64:
		w.WriteString(fmt.Sprintf("%f", v))
	case bool:
		w.WriteBool(v)
	case []string:
		w.WriteStrArray(v)
	case map[string]int:
		w.WriteStrIntMap(v)
	case map[string]uint64:
		w.WriteStrUintMap(v)
	case time.Time:
		w.WriteTimestamp(v)
	case nil:
		w.WriteNull()
	default:
		return false
	}
	return true
}

// Write a boolean field.
func (w *RowWriter) WriteBool(v bool) {
	if v {
		w.buf.WriteString("TRUE")
	} else {
		w.buf.WriteString("FALSE")
	}
	w.buf.WriteByte(w.fieldDelimiter)
}

// Write an integer field.
func (w *RowWriter) WriteInt(v int) {
	w.buf.WriteString(strconv.Itoa(v))
	w.buf.WriteByte(w.fieldDelimiter)
}

// Writes a properly escaped string field.
func (w *RowWriter) WriteString(v string) {
	w.writeString(v)
	w.buf.WriteByte(w.fieldDelimiter)
}

// Main logic of WriteString but doesn't write field delimiter so maps and
// arrays can use it.
func (w *RowWriter) writeString(v string) {
	// Write string after replacing delimiters with their escaped form.
	w.buf.WriteString(w.replacer.Replace(v))
}

// Write a time as a Hive formatted timestamp.
func (w *RowWriter) WriteTimestamp(v time.Time) {
	w.writeString(v.Format(TimestampFormat))
	w.buf.WriteByte(w.fieldDelimiter)
}

// Write an empty field (NULL in Hive).
func (w *RowWriter) WriteNull() {
	w.buf.WriteByte(w.fieldDelimiter)
}

// Write a []string field.
func (w *RowWriter) WriteStrArray(array []string) {
	for i, item := range array {
		if i > 0 {
			w.buf.WriteByte(w.itemDelimiter)
		}
		w.writeString(item)
	}
	w.buf.WriteByte(w.fieldDelimiter)
}

// Write a []int field.
func (w *RowWriter) WriteIntArray(array []int) {
	for i, item := range array {
		if i > 0 {
			w.buf.WriteByte(w.itemDelimiter)
		}
		w.buf.WriteString(strconv.Itoa(item))
	}
	w.buf.WriteByte(w.fieldDelimiter)
}

// Write a map[string]int field.
func (w *RowWriter) WriteStrIntMap(m map[string]int) {
	first := true
	for k, v := range m {
		if first {
			first = false
		} else {
			w.buf.WriteByte(w.itemDelimiter)
		}
		w.writeString(k)
		w.buf.WriteByte(w.mapKeyDelimiter)
		w.buf.WriteString(strconv.Itoa(v))
	}
	w.buf.WriteByte(w.fieldDelimiter)
}

// Write a map[string]uint64 field.
func (w *RowWriter) WriteStrUintMap(m map[string]uint64) {
	first := true
	for k, v := range m {
		if first {
			first = false
		} else {
			w.buf.WriteByte(w.itemDelimiter)
		}
		w.writeString(k)
		w.buf.WriteByte(w.mapKeyDelimiter)
		w.buf.WriteString(strconv.FormatUint(v, 10))
	}
	w.buf.WriteByte(w.fieldDelimiter)
}

// Returns the current row and resets the internal buffer for the next row.
func (w *RowWriter) Row() []byte {
	w.buf.WriteByte(w.lineEnding)
	buf := make([]byte, w.buf.Len())
	w.buf.Read(buf)
	return buf
}

// Drop the current row (resets the internal row buffer).
func (w *RowWriter) Reset() {
	w.buf.Reset()
}
