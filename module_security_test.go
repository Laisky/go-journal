package journal

import (
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/mod/semver"
)

// These floors protect the full module graph scanned by SCM security tools,
// including packages not imported by this runtime. They are not reachability claims.
var moduleSecurityFloors = map[string]string{
	"github.com/yuin/goldmark": "v1.7.17", // GO-2026-5320
	"golang.org/x/net":         "v0.60.0", // GO-2026-6603 and related HTTP/2 fixes
	"golang.org/x/text":        "v0.42.0", // retains the previously qualified floor; GO-2026-6629 fixed in v0.41.0
	"golang.org/x/crypto":      "v0.57.0", // preserve the previous qualified graph, without claiming openpgp is safe
}

func secureModuleVersion(version, minimum string) bool {
	return semver.IsValid(version) && semver.Compare(version, minimum) >= 0
}

func TestModuleGraphSecurityFloors(t *testing.T) {
	output, err := exec.Command("go", "list", "-mod=readonly", "-m", "-json", "all").CombinedOutput()
	if err != nil {
		t.Fatalf("resolve actual module graph: %v\n%s", err, output)
	}
	selected := map[string]string{}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	for {
		var module struct{ Path, Version string }
		if err := decoder.Decode(&module); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		selected[module.Path] = module.Version
	}
	for path, minimum := range moduleSecurityFloors {
		t.Run(path, func(t *testing.T) {
			version, present := selected[path]
			if !present {
				return
			} // Removing an unused module does not require adding it back.
			if !secureModuleVersion(version, minimum) {
				t.Fatalf("selected %s@%s falls below security floor %s", path, version, minimum)
			}
		})
	}
}

func TestSecurityFloorComparison(t *testing.T) {
	for _, tc := range []struct {
		name, version, minimum string
		want                   bool
	}{
		{"older", "v1.4.13", "v1.7.17", false},
		{"fixed", "v1.7.17", "v1.7.17", true},
		{"newer", "v1.8.0", "v1.7.17", true},
		{"prerelease", "v1.7.17-rc.1", "v1.7.17", false},
		{"invalid", "", "v1.7.17", false},
		{"oldpseudo", "v0.0.0-20210921155107-089bfa567519", "v0.57.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := secureModuleVersion(tc.version, tc.minimum); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
