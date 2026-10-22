package dependencysecurity_test

import (
	"bytes"
	"github.com/yuin/goldmark"
	"strings"
	"testing"
)

func TestGoldmarkRejectsEncodedDangerousScheme(t *testing.T) {
	var out bytes.Buffer
	if err := goldmark.Convert([]byte("[x](javascript&colon;alert(1))"), &out); err != nil {
		t.Fatal(err)
	}
	html := strings.ToLower(out.String())
	if strings.Contains(html, "href=\"javascript") {
		t.Fatalf("encoded unsafe scheme survived validation: %s", out.String())
	}
	if err := goldmark.Convert([]byte("[ok](https://example.invalid/path)"), &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "https://example.invalid/path") {
		t.Fatal("ordinary link changed")
	}
}
