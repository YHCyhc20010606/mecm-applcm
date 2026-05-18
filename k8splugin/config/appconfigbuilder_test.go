package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInjectResourceRequests(t *testing.T) {
	values := map[string]interface{}{}
	parameters := map[string]string{
		"cpuRequest":    "2",
		"memoryRequest": "2Gi",
	}

	injectResourceRequests(values, parameters)

	resources := values["resources"].(map[string]interface{})
	requests := resources["requests"].(map[string]interface{})
	if requests["cpu"] != "2" {
		t.Fatalf("expected cpu request 2, got %v", requests["cpu"])
	}
	if requests["memory"] != "2Gi" {
		t.Fatalf("expected memory request 2Gi, got %v", requests["memory"])
	}
}

func TestInjectResourceRequestsNormalizesMemoryMi(t *testing.T) {
	values := map[string]interface{}{}
	parameters := map[string]string{
		"cpuRequest":    "0.5",
		"memoryRequest": "512",
	}

	injectResourceRequests(values, parameters)

	resources := values["resources"].(map[string]interface{})
	requests := resources["requests"].(map[string]interface{})
	if requests["cpu"] != "0.5" {
		t.Fatalf("expected cpu request 0.5, got %v", requests["cpu"])
	}
	if requests["memory"] != "512Mi" {
		t.Fatalf("expected memory request 512Mi, got %v", requests["memory"])
	}
}

func TestInjectResourceRequestsSkipsPartialInput(t *testing.T) {
	values := map[string]interface{}{}
	parameters := map[string]string{
		"cpuRequest": "2",
	}

	injectResourceRequests(values, parameters)

	if _, ok := values["resources"]; ok {
		t.Fatal("expected partial resource request to be skipped")
	}
}

func TestInjectResourceRequestsSkipsInvalidInput(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]string
	}{
		{
			name: "invalid cpu",
			parameters: map[string]string{
				"cpuRequest":    "0",
				"memoryRequest": "2Gi",
			},
		},
		{
			name: "invalid memory",
			parameters: map[string]string{
				"cpuRequest":    "2",
				"memoryRequest": "0Mi",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := map[string]interface{}{}

			injectResourceRequests(values, test.parameters)

			if _, ok := values["resources"]; ok {
				t.Fatal("expected invalid resource request to be skipped")
			}
		})
	}
}

func TestInjectResourceRequestsMaxBoundary(t *testing.T) {
	values := map[string]interface{}{}
	parameters := map[string]string{
		"cpuRequest":    "100",
		"memoryRequest": "128Gi",
	}
	injectResourceRequests(values, parameters)
	resources := values["resources"].(map[string]interface{})
	requests := resources["requests"].(map[string]interface{})
	if requests["cpu"] != "100" || requests["memory"] != "128Gi" {
		t.Fatalf("unexpected requests: %v", requests)
	}

	values2 := map[string]interface{}{}
	parameters2 := map[string]string{
		"cpuRequest":    "1",
		"memoryRequest": "131072",
	}
	injectResourceRequests(values2, parameters2)
	r2 := values2["resources"].(map[string]interface{})["requests"].(map[string]interface{})
	if r2["memory"] != "131072Mi" {
		t.Fatalf("expected 131072Mi, got %v", r2["memory"])
	}
}

func TestInjectResourceRequestsSkipsOverMax(t *testing.T) {
	tests := []map[string]string{
		{"cpuRequest": "100.1", "memoryRequest": "2Gi"},
		{"cpuRequest": "2", "memoryRequest": "129Gi"},
		{"cpuRequest": "2", "memoryRequest": "131073"},
	}
	for _, p := range tests {
		values := map[string]interface{}{}
		injectResourceRequests(values, p)
		if _, ok := values["resources"]; ok {
			t.Fatalf("expected skip for params %+v", p)
		}
	}
}

func TestInjectResourceRequestsIntoTemplatesPreservesIndent(t *testing.T) {
	dir := t.TempDir()
	templatesDir := filepath.Join(dir, "templates")
	if err := os.MkdirAll(templatesDir, 0755); err != nil {
		t.Fatalf("mkdir templates: %v", err)
	}

	// The resources placeholder is intentionally indented to simulate container-level indentation.
	input := "apiVersion: v1\nkind: Pod\nspec:\n  containers:\n  - name: x\n    image: busybox\n    resources: {}\n"
	path := filepath.Join(templatesDir, "pod.yaml")
	if err := os.WriteFile(path, []byte(input), 0644); err != nil {
		t.Fatalf("write template: %v", err)
	}

	params := map[string]string{
		"cpuRequest":    "2",
		"memoryRequest": "2Gi",
	}
	if err := injectResourceRequestsIntoTemplates(dir, params); err != nil {
		t.Fatalf("injectResourceRequestsIntoTemplates err: %v", err)
	}

	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	got := string(out)

	// Expect exact indentation relative to the original "resources: {}" line.
	if !strings.Contains(got, "    resources:\n      requests:\n        cpu: \"2\"\n        memory: \"2Gi\"") {
		t.Fatalf("unexpected output:\n%s", got)
	}
}

func TestInjectResourceRequestsPreservesExistingLimits(t *testing.T) {
	values := map[string]interface{}{
		"resources": map[string]interface{}{
			"limits": map[string]interface{}{
				"cpu": "4",
			},
		},
	}
	parameters := map[string]string{
		"cpuRequest":    "2",
		"memoryRequest": "2048Mi",
	}

	injectResourceRequests(values, parameters)

	resources := values["resources"].(map[string]interface{})
	if _, ok := resources["limits"]; !ok {
		t.Fatal("expected existing limits to be preserved")
	}
	requests := resources["requests"].(map[string]interface{})
	if requests["cpu"] != "2" || requests["memory"] != "2048Mi" {
		t.Fatalf("unexpected requests: %v", requests)
	}
}

func TestInjectHostPathMount(t *testing.T) {
	values := map[string]interface{}{}
	parameters := map[string]string{
		"hostPathMountType":   "hostPath",
		"hostPath":            "/home/mec/mountpath",
		"containerMountPath": "/usr/app/mountpath",
		"volumeName":          "myapp-volume",
	}

	injectHostPathMount(values, parameters)

	hm := values["hostPathExtraMount"].(map[string]interface{})
	if hm["enabled"] != true {
		t.Fatalf("expected enabled true")
	}
	if hm["name"] != "myapp-volume" || hm["hostPath"] != "/home/mec/mountpath" ||
		hm["mountPath"] != "/usr/app/mountpath" || hm["type"] != "DirectoryOrCreate" {
		t.Fatalf("unexpected hostPathExtraMount: %v", hm)
	}
}

func TestInjectHostPathMountSkipsNonHostPath(t *testing.T) {
	values := map[string]interface{}{}
	injectHostPathMount(values, map[string]string{
		"hostPathMountType": "",
		"hostPath":          "/a",
		"containerMountPath": "/b",
		"volumeName":        "v-volume",
	})
	if _, ok := values["hostPathExtraMount"]; ok {
		t.Fatal("expected skip")
	}
}

func TestInjectHostPathMountSkipsInvalidVolumeName(t *testing.T) {
	values := map[string]interface{}{}
	injectHostPathMount(values, map[string]string{
		"hostPathMountType":   "hostPath",
		"hostPath":            "/a",
		"containerMountPath": "/b",
		"volumeName":          "Bad_Name",
	})
	if _, ok := values["hostPathExtraMount"]; ok {
		t.Fatal("expected skip invalid name")
	}
}

func TestInjectHostPathMountSkipsRelativePaths(t *testing.T) {
	values := map[string]interface{}{}
	injectHostPathMount(values, map[string]string{
		"hostPathMountType":   "hostPath",
		"hostPath":            "relative",
		"containerMountPath": "/b",
		"volumeName":          "v-volume",
	})
	if _, ok := values["hostPathExtraMount"]; ok {
		t.Fatal("expected skip relative host path")
	}
}
