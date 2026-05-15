package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/intstr"
	server "k8splugin/pkg/server"

	v1 "k8s.io/api/core/v1"
)

func TestBuildAppPortsJson(t *testing.T) {
	pods := &v1.PodList{
		Items: []v1.Pod{
			{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "c1",
							Ports: []v1.ContainerPort{
								{ContainerPort: 4040, Protocol: v1.ProtocolTCP, Name: "http"},
							},
						},
					},
				},
				Status: v1.PodStatus{},
				ObjectMeta: v1.ObjectMeta{
					Name: "pod-1",
				},
			},
		},
	}

	services := &v1.ServiceList{
		Items: []v1.Service{
			{
				ObjectMeta: v1.ObjectMeta{Name: "svc-1"},
				Spec: v1.ServiceSpec{
					Type: v1.ServiceTypeClusterIP,
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       80,
							TargetPort: intstr.FromInt(4040),
							NodePort:   0,
						},
					},
				},
			},
		},
	}

	portsJson, err := server.BuildAppPortsJson(pods, services)
	assert.NoError(t, err)
	assert.Contains(t, portsJson, "\"containerPorts\"")
	assert.Contains(t, portsJson, "\"servicePorts\"")
	assert.Contains(t, portsJson, "4040")
	assert.Contains(t, portsJson, "80")
}

func TestBuildAppPortsJson_Empty(t *testing.T) {
	portsJson, err := server.BuildAppPortsJson(&v1.PodList{}, &v1.ServiceList{})
	assert.Error(t, err)
	assert.Equal(t, "", portsJson)
}

