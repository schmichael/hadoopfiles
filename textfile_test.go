package hadoopfiles

import (
	"bytes"
	"testing"
	"time"
)

func TestRowWriter(t *testing.T) {
	// Maps are unordered, so support both World1 first and World2 first
	f := NewRowWriter()
	{
		var (
			expected1 = []byte("AA\x0199\x01B1\x034\x02B2\x035\x0166\x0277\x01CC\x02DD\x01\n")
			expected2 = []byte("AA\x0199\x01B2\x035\x02B1\x034\x0166\x0277\x01CC\x02DD\x01\n")
		)
		f.WriteString("AA")
		f.WriteInt(99)
		f.WriteStrIntMap(map[string]int{"B1": 4, "B2": 5})
		f.WriteIntArray([]int{66, 77})
		f.WriteStrArray([]string{"CC", "DD"})
		out := f.Row()
		if !bytes.Equal(out, expected1) && !bytes.Equal(out, expected2) {
			t.Errorf("Neither expected output matched:\n%q !=\n%q\n\n%q !=\n%q", out, expected1, out, expected2)
		}
	}

	{
		expected := []byte("newrow\x01\n")
		f.WriteString("newrow")
		out := f.Row()
		if !bytes.Equal(out, expected) {
			t.Fatalf("Expected: %q !=\nActual:  %q", expected, out)
		}
	}

	{
		expected := []byte("2014-01-02 03:04:05.666666666\x01\n")
		f.WriteTimestamp(time.Date(2014, 1, 2, 3, 4, 5, 666666666, time.UTC))
		out := f.Row()
		if !bytes.Equal(out, expected) {
			t.Fatalf("Expected: %q !=\nActual:  %q", expected, out)
		}
	}

	{
		expected := []byte("5.500000\x01\n")
		f.WriteField(5.5)
		out := f.Row()
		if !bytes.Equal(out, expected) {
			t.Fatalf("Expected: %q !=\nActual:  %q", expected, out)
		}
	}
}

func TestRowWriterDelimiters(t *testing.T) {
	f := NewRowWriter()

	for _, bd := range []byte{'\x02', 'a', '1', 'U', '\xf0'} {
		if err := f.SetDelimiters(bd, '\x02', '\x03', '\n'); err == nil {
			t.Errorf("SetDelimiters should have failed on '%q' but did not.", bd)
		}
		if f.delims != "\x01\x02\x03\n" {
			t.Errorf("delims shouldn't have changed: %s", f.delims)
		}
	}

	// Try CSVish options
	if err := f.SetDelimiters(',', ';', ':', '\n'); err != nil {
		t.Fatal(err)
	}
	{
		expected := []byte("['\x01'\x02\x03\\,\\;\\:\\\\],1;2;3,key:1,\n")
		f.WriteString("['\x01'\x02\x03,;:\\]")
		f.WriteIntArray([]int{1,2,3})
		f.WriteStrIntMap(map[string]int{"key": 1})
		out := f.Row()
		if !bytes.Equal(out, expected) {
			t.Fatalf("\nExpected: %q !=\nActual:   %q", expected, out)
		}
	}
}
