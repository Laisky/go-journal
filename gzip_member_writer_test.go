package journal

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

func TestGzipMembersRemainCompleteAcrossFlush(t *testing.T) {
	var output bytes.Buffer
	writer, err := newGzipMemberWriter(&output)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first", "second"} {
		if _, err := writer.Write([]byte(value)); err != nil {
			t.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := gzip.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "firstsecond" {
		t.Fatalf("members changed: %q", data)
	}
}
func TestGzipFooterRemainsBufferedUntilFlush(t *testing.T) {
	var output bytes.Buffer
	writer, err := newGzipMemberWriter(&output)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteFooter(); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 || writer.output.Buffered() == 0 {
		t.Fatal("footer bypassed the existing buffer boundary")
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "retained" {
		t.Fatalf("incomplete member: %q %v", data, err)
	}
}

type failingGzipOutput struct{}

func (failingGzipOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestGzipFlushDoesNotAcknowledgeFailedOutput(t *testing.T) {
	writer, err := newGzipMemberWriter(failingGzipOutput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Flush(); err != io.ErrClosedPipe {
		t.Fatalf("lost flush failure: %v", err)
	}
}
