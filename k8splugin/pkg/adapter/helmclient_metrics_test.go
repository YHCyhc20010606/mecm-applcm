package adapter

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetNodeAllocatableCpuMemSumsAllNodes(t *testing.T) {
	nodeList := &v1.NodeList{Items: []v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1500m"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "node-2"},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
		},
	}}

	totalCpu, totalMem := getNodeAllocatableCpuMemFromList(nodeList)

	if totalCpu != 3500000000 {
		t.Fatalf("unexpected cpu total: got %d", totalCpu)
	}
	if totalMem != 2684354560 {
		t.Fatalf("unexpected memory total: got %d", totalMem)
	}
}

func TestGetNodeTotalCpuMemParsesQuantityUnits(t *testing.T) {
	statsInfo := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"usage": map[string]interface{}{
					"cpu":    "250m",
					"memory": "128Mi",
				},
			},
			map[string]interface{}{
				"usage": map[string]interface{}{
					"cpu":    "1000000n",
					"memory": "1024Ki",
				},
			},
			map[string]interface{}{
				"usage": map[string]interface{}{
					"cpu":    "2",
					"memory": "2048",
				},
			},
		},
	}

	totalCpu, totalMem := getNodeTotalCpuMem(statsInfo)

	if totalCpu != 2251000000 {
		t.Fatalf("unexpected cpu usage: got %d", totalCpu)
	}
	if totalMem != 135268352 {
		t.Fatalf("unexpected memory usage: got %d", totalMem)
	}
}
