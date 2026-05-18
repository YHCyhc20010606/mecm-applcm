/*
 * Copyright 2020 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package server

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/tap"
	"io"
	"io/ioutil"
	"k8splugin/conf"
	"k8splugin/internal/internal_lcmservice"
	"k8splugin/models"
	"k8splugin/pgdb"
	"k8splugin/pkg/adapter"
	"k8splugin/util"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	KubeconfigPath = "/usr/app/artifacts/config/"
	appPackagesBasePath = "/usr/app/artifacts/packages/"
)

// GRPC server
type ServerGRPC struct {
	server       *grpc.Server
	port         string
	address      string
	certificate  string
	key          string
	db           pgdb.Database
	serverConfig *conf.ServerConfigurations
}

// GRPC service configuration used to create GRPC server
type ServerGRPCConfig struct {
	Port         string
	Address      string
	ServerConfig *conf.ServerConfigurations
}

// Rate Limit
type RateLimit struct {
	lim *rate.Limiter
}

// New Rate Limit constructor
func NewRateLimit() *RateLimit {
	return &RateLimit{rate.NewLimiter(1, 200)}
}

// Constructor to GRPC server
func NewServerGRPC(cfg ServerGRPCConfig) (s ServerGRPC) {
	s.port = cfg.Port
	s.address = cfg.Address
	s.certificate = cfg.ServerConfig.CertFilePath
	s.key = cfg.ServerConfig.KeyFilePath
	s.serverConfig = cfg.ServerConfig
	dbAdapter, err := pgdb.GetDbAdapter(cfg.ServerConfig)
	if err != nil {
		log.Error("Failed to get database")
		os.Exit(1)
	}
	s.db = dbAdapter
	log.Infof("Binding is successful")
	return
}

// Start GRPC server and start listening on the port
func (s *ServerGRPC) Listen() (err error) {
	var (
		listener net.Listener
	)

	// Listen announces on the network address
	listener, err = net.Listen("tcp", s.address+":"+s.port)
	if err != nil {
		log.Error("failed to listen on specified port")
		return err
	}

	if !s.serverConfig.SslNotEnabled {
		tlsConfig, err := util.GetTLSConfig(s.serverConfig, s.certificate, s.key)
		if err != nil {
			log.Error("failed to load certificates")
			return err
		}

		// Create the TLS credentials
		creds := credentials.NewTLS(tlsConfig)

		// Create server with TLS credentials
		s.server = grpc.NewServer(grpc.Creds(creds), grpc.InTapHandle(NewRateLimit().Handler))
	} else {
		// Create server without TLS credentials
		s.server = grpc.NewServer(grpc.InTapHandle(NewRateLimit().Handler))
	}

	internal_lcmservice.RegisterAppLCMServer(s.server, s)
	log.Infof("Server registered with GRPC")

	// Server start serving
	err = s.server.Serve(listener)
	if err != nil {
		log.Error("failed to listen for GRPC connections.")
		return err
	}
	log.Info("Server started listening on configured port")
	return
}

// Handler to check service is over rate limit or not
func (t *RateLimit) Handler(ctx context.Context, info *tap.Info) (context.Context, error) {
	if !t.lim.Allow() {
		return nil, status.Errorf(codes.ResourceExhausted, "service is over rate limit")
	}
	return ctx, nil
}

// Pod Description
func (s *ServerGRPC) WorkloadEvents(ctx context.Context, req *internal_lcmservice.WorkloadEventsRequest) (resp *internal_lcmservice.WorkloadEventsResponse, err error) {

	resp = &internal_lcmservice.WorkloadEventsResponse{
		Response: util.Failure,
	}

	err = s.displayReceivedMsg(ctx, util.WorkloadEvents)
	if err != nil {
		s.displayResponseMsg(ctx, util.WorkloadEvents, util.FailedToDispRecvMsg)
		return resp, err
	}

	// Input validation
	hostIp, appInsId, tenantId, err := s.validateInputParamsForPodDesc(req)
	if err != nil {
		s.displayResponseMsg(ctx, util.WorkloadEvents, util.FailedToValInputParams)
		return resp, err
	}

	// Get Client
	client, err := adapter.GetClient(util.DeployType, tenantId, hostIp)
	if err != nil {
		s.displayResponseMsg(ctx, util.WorkloadEvents, util.FailedToGetClient)
		return resp, err
	}

	appInstanceRecord := &models.AppInstanceInfo{
		AppInsId: appInsId,
	}
	readErr := s.db.ReadData(appInstanceRecord, util.AppInsId)
	if readErr != nil {
		log.Error(util.AppRecordDoesNotExit)
		s.displayResponseMsg(ctx, util.Query, util.AppRecordDoesNotExit)
		return resp, err
	}

	// Query Chart
	r, err := client.WorkloadEvents(appInstanceRecord.WorkloadId, appInstanceRecord.Namespace)
	if err != nil {
		s.displayResponseMsg(ctx, util.WorkloadEvents, "failed to get pod describe information")
		return resp, err
	}
	resp = &internal_lcmservice.WorkloadEventsResponse{
		Response: r,
	}
	s.handleLoggingForSuccess(ctx, util.WorkloadEvents, "Pod description is successful")
	return resp, nil
}

// Query application
func (s *ServerGRPC) Query(ctx context.Context, req *internal_lcmservice.QueryRequest) (resp *internal_lcmservice.QueryResponse, err error) {

	resp = &internal_lcmservice.QueryResponse{
		Response: util.Failure,
	}

	err = s.displayReceivedMsg(ctx, util.Query)
	if err != nil {
		s.displayResponseMsg(ctx, util.Query, util.FailedToDispRecvMsg)
		return resp, err
	}

	// Input validation
	tenantId, hostIp, appInsId, err := s.validateInputParamsForQuery(req)
	if err != nil {
		s.displayResponseMsg(ctx, util.Query, util.FailedToValInputParams)
		return resp, err
	}

	// Get Client
	client, err := adapter.GetClient(util.DeployType, tenantId, hostIp)
	if err != nil {
		s.displayResponseMsg(ctx, util.Query, util.FailedToGetClient)
		return resp, err
	}

	appInstanceRecord := &models.AppInstanceInfo{
		AppInsId: appInsId,
	}
	readErr := s.db.ReadData(appInstanceRecord, util.AppInsId)
	if readErr != nil {
		log.Error(util.AppRecordDoesNotExit)
		s.displayResponseMsg(ctx, util.Query, util.AppRecordDoesNotExit)
		return resp, err
	}

	// Query Chart
	r, err := client.Query(appInstanceRecord.WorkloadId, appInstanceRecord.Namespace)
	if err != nil {
		log.Errorf("Chart not found for workloadId: %s. Err: %s", appInstanceRecord.WorkloadId, err)
		s.displayResponseMsg(ctx, util.Query, "chart not found for workloadId")
		return resp, err
	}
	resp = &internal_lcmservice.QueryResponse{
		Response: r,
	}
	s.handleLoggingForSuccess(ctx, util.Query, "Query pod statistics is successful")
	return resp, nil
}

// QueryKPI application
func (s *ServerGRPC) QueryKPI(ctx context.Context, req *internal_lcmservice.QueryKPIRequest) (resp *internal_lcmservice.QueryKPIResponse, err error) {

	resp = &internal_lcmservice.QueryKPIResponse{
		Response: util.Failure,
	}

	err = s.displayReceivedMsg(ctx, util.QueryKPI)
	if err != nil {
		s.displayResponseMsg(ctx, util.QueryKPI, util.FailedToDispRecvMsg)
		return resp, err
	}

	// Input validation
	tenantId, hostIp, err := s.validateInputParamsForQueryKPI(req)
	if err != nil {
		s.displayResponseMsg(ctx, util.QueryKPI, util.FailedToValInputParams)
		return resp, err
	}

	// Get Client
	client, err := adapter.GetClient(util.DeployType, tenantId, hostIp)
	if err != nil {
		s.displayResponseMsg(ctx, util.QueryKPI, util.FailedToGetClient)
		return resp, err
	}

	// Query KPI information
	r, err := client.QueryKPI()
	if err != nil {
		s.displayResponseMsg(ctx, util.QueryKPI, "Query kpi statistics is failed")
		return resp, err
	}
	resp = &internal_lcmservice.QueryKPIResponse{
		Response: r,
	}
	s.handleLoggingForSuccess(ctx, util.QueryKPI, "Query kpi statistics is successful")
	return resp, nil
}

// Terminate application
func (s *ServerGRPC) Terminate(ctx context.Context,
	req *internal_lcmservice.TerminateRequest) (resp *internal_lcmservice.TerminateResponse, err error) {

	resp = &internal_lcmservice.TerminateResponse{
		Status: util.Failure,
	}

	err = s.displayReceivedMsg(ctx, util.Terminate)
	if err != nil {
		s.displayResponseMsg(ctx, util.Terminate, util.FailedToDispRecvMsg)
		return resp, err
	}

	tenantId, hostIp, appInsId, err := s.validateInputParamsForTerm(req)
	if err != nil {
		s.displayResponseMsg(ctx, util.Terminate, util.FailedToValInputParams)
		return resp, err
	}
	appInstanceRecord := &models.AppInstanceInfo{
		AppInsId: appInsId,
	}
	readErr := s.db.ReadData(appInstanceRecord, util.AppInsId)
	if readErr != nil {
		log.Error(util.AppRecordDoesNotExit)
		s.displayResponseMsg(ctx, util.Terminate, util.AppRecordDoesNotExit)
		return resp, err
	}

	// Get Client
	client, err := adapter.GetClient(util.DeployType,tenantId, hostIp)
	if err != nil {
		s.displayResponseMsg(ctx, util.Terminate, util.FailedToGetClient)
		return resp, err
	}

	// Uninstall chart
	err = client.UnDeploy(appInstanceRecord.WorkloadId, appInstanceRecord.Namespace)
	if err != nil {
		log.Errorf("Chart not found for workloadId: %s. Err: %s", appInstanceRecord.WorkloadId, err)
		s.displayResponseMsg(ctx, util.Terminate, "chart not found for workloadId")
		return resp, err
	}
	err = s.deleteAppInfoRecord(appInsId)
	if err != nil {
		s.displayResponseMsg(ctx, util.Terminate, "failed to delete app info record from database")
		return resp, err
	}
	resp = &internal_lcmservice.TerminateResponse{
		Status: util.Success,
	}

	s.handleLoggingForSuccess(ctx, util.Terminate, "Termination is successful")
	return resp, nil
}



func (s *ServerGRPC) Instantiate(ctx context.Context,
	req *internal_lcmservice.InstantiateRequest) (resp *internal_lcmservice.InstantiateResponse, err error) {

	resp = &internal_lcmservice.InstantiateResponse{
		Status: util.Failure,
	}

	err = s.displayReceivedMsg(ctx, util.Instantiate)
	if err != nil {
		s.displayResponseMsg(ctx, util.Instantiate, util.FailedToDispRecvMsg)
		return resp, err
	}

	tenantId, packageId, hostIp, appInsId, ak, sk, err := s.validateInputParamsForInstantiate(req)
	if err != nil {
		s.displayResponseMsg(ctx, util.Instantiate, util.FailedToValInputParams)
		return resp, err
	}
	// [NetworkPlane DEBUG] 节点4: k8splugin gRPC server 收到请求，打印 parameters
	log.Infof("[NetworkPlane] [4/5] k8splugin grpcserver Instantiate, appInsId=%s, parameters count=%d", appInsId, len(req.Parameters))
	for k, v := range req.Parameters {
		log.Infof("[NetworkPlane] [4/5]   param key=%s  value=%s", k, v)
	}
	if _, ok := req.Parameters["networkPlane"]; !ok {
		log.Warn("[NetworkPlane] [4/5]   WARNING: key 'networkPlane' NOT found, will use default network")
	}
	appPkgRecord := &models.AppPackage{
		AppPkgId: packageId + tenantId + hostIp,
	}
	readErr := s.db.ReadData(appPkgRecord, util.AppPkgId)
	if readErr != nil {
		log.Error(util.AppPkgRecordDoesNotExit)
		s.displayResponseMsg(ctx, util.Instantiate, util.AppPkgRecordDoesNotExit)
		return resp, err
	}

	// Get Client
	client, err := adapter.GetClient(util.DeployType, tenantId, hostIp)
	if err != nil {
		s.displayResponseMsg(ctx, util.Instantiate, util.FailedToGetClient)
		return resp, err
	}

	// [修改] 2026-01-19 网络平面功能：将前端传递的 parameters 参数传递给 Deploy 方法
	// 原始调用: client.Deploy(appPkgRecord, appInsId, ak, sk, s.db)
	// 新增参数: req.Parameters - 包含 networkPlane 等前端配置
	releaseName, namespace, err := client.Deploy(appPkgRecord, appInsId, ak, sk, req.Parameters, s.db)
	if err != nil {
		log.Info("instantiation failed")
		s.displayResponseMsg(ctx, util.Instantiate, "instantiation failed")
		return resp, err
	}

	networkPlane := util.Default
	if req.Parameters != nil {
		if selectedNetwork, ok := req.Parameters["networkPlane"]; ok && selectedNetwork != "" {
			networkPlane = selectedNetwork
		}
	}
	err = s.insertOrUpdateAppInsRecord(appInsId, hostIp, releaseName, namespace, "")
	if err != nil {
		s.displayResponseMsg(ctx, util.Instantiate, "failed to insert or update app record")
		return resp, err
	}

	go s.pollAndUpdateAppIp(appInsId, tenantId, hostIp, namespace, releaseName, networkPlane)

	log.Info("successful instantiation")
	resp.Status = util.Success
	s.handleLoggingForSuccess(ctx, util.Instantiate, "Application instantiated successfully")
	return resp, nil
}

func (s *ServerGRPC) pollAndUpdateAppIp(appInsId, tenantId, hostIp, namespace, releaseName, networkPlane string) {
	const (
		maxWaitSeconds = 600
		retryInterval  = 2 * time.Second
	)

	start := time.Now()
	attempt := 0
	for time.Since(start) < maxWaitSeconds*time.Second {
		attempt++
		log.Infof("[AppIP] polling start appInsId=%s releaseName=%s namespace=%s attempt=%d networkPlane=%s",
			appInsId, releaseName, namespace, attempt, networkPlane)
		appIp, appIpErr := s.getAppIpByNetworkPlane(tenantId, hostIp, namespace, releaseName, networkPlane)
		if appIpErr == nil && appIp != "" {
			updateErr := s.insertOrUpdateAppInsRecord(appInsId, hostIp, releaseName, namespace, appIp)
			if updateErr != nil {
				log.Warnf("found app ip %s but failed to update app_instance_info, appInsId=%s, err=%v", appIp, appInsId, updateErr)
				return
			}
			log.Infof("app ip updated successfully, appInsId=%s, appIp=%s", appInsId, appIp)
			pushErr := s.pushAppIpToAppo(tenantId, appInsId, appIp)
			if pushErr != nil {
				log.Warnf("[AppIPPush] push failed appInsId=%s appIp=%s err=%v", appInsId, appIp, pushErr)
			} else {
				log.Infof("[AppIPPush] push success appInsId=%s appIp=%s", appInsId, appIp)
			}
			portsJson, portsErr := s.getAppPortsJson(tenantId, hostIp, namespace, releaseName)
			if portsErr != nil {
				log.Infof("[AppPorts] collect failed appInsId=%s releaseName=%s attempt=%d err=%v",
					appInsId, releaseName, attempt, portsErr)
			} else if portsJson != "" {
				portsPushErr := s.pushAppPortsToAppo(tenantId, appInsId, portsJson)
				if portsPushErr != nil {
					log.Warnf("[AppPortsPush] push failed appInsId=%s err=%v", appInsId, portsPushErr)
				} else {
					log.Infof("[AppPortsPush] push success appInsId=%s", appInsId)
				}
			}
			return
		}
		if appIpErr != nil {
			log.Infof("[AppIP] polling no-result appInsId=%s releaseName=%s attempt=%d err=%v", appInsId, releaseName, attempt, appIpErr)
		}

		time.Sleep(retryInterval)
	}

	log.Warnf("timeout waiting app ip from network-status/pod ip, appInsId=%s, releaseName=%s", appInsId, releaseName)
}

func (s *ServerGRPC) getAppPortsJson(tenantId, hostIp, namespace, releaseName string) (string, error) {
	kubeConfigPath := KubeconfigPath + tenantId + "/" + hostIp
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return "", err
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return "", err
	}

	pods, err := s.getReleasePods(clientset, namespace, releaseName)
	if err != nil {
		return "", err
	}

	services, err := s.getReleaseServices(clientset, namespace, releaseName)
	if err != nil {
		return "", err
	}
	portsJson, buildErr := BuildAppPortsJson(pods, services)
	if buildErr == nil {
		log.Infof("[AppPorts] collected releaseName=%s namespace=%s containerPorts=%d servicePorts=%d jsonBytes=%d",
			releaseName, namespace, countContainerPorts(pods), countServicePorts(services), len(portsJson))
	}
	return portsJson, buildErr
}

func countContainerPorts(pods *v1.PodList) int {
	count := 0
	if pods == nil {
		return 0
	}
	for _, pod := range pods.Items {
		for _, c := range pod.Spec.Containers {
			count += len(c.Ports)
		}
	}
	return count
}

func countServicePorts(services *v1.ServiceList) int {
	count := 0
	if services == nil {
		return 0
	}
	for _, svc := range services.Items {
		count += len(svc.Spec.Ports)
	}
	return count
}

func BuildAppPortsJson(pods *v1.PodList, services *v1.ServiceList) (string, error) {
	type containerPortEntry struct {
		Pod       string `json:"pod"`
		Container string `json:"container"`
		Port      int32  `json:"port"`
		Protocol  string `json:"protocol"`
		Name      string `json:"name"`
	}
	type servicePortEntry struct {
		Service    string `json:"service"`
		Type       string `json:"type"`
		Port       int32  `json:"port"`
		TargetPort string `json:"targetPort"`
		NodePort   int32  `json:"nodePort"`
		Protocol   string `json:"protocol"`
		Name       string `json:"name"`
	}
	type appPortsPayload struct {
		ContainerPorts []containerPortEntry `json:"containerPorts"`
		ServicePorts   []servicePortEntry   `json:"servicePorts"`
	}

	payload := appPortsPayload{
		ContainerPorts: make([]containerPortEntry, 0),
		ServicePorts:   make([]servicePortEntry, 0),
	}

	if pods != nil {
		for _, pod := range pods.Items {
			for _, c := range pod.Spec.Containers {
				for _, p := range c.Ports {
					proto := string(p.Protocol)
					if proto == "" {
						proto = "TCP"
					}
					payload.ContainerPorts = append(payload.ContainerPorts, containerPortEntry{
						Pod:       pod.Name,
						Container: c.Name,
						Port:      p.ContainerPort,
						Protocol:  proto,
						Name:      p.Name,
					})
				}
			}
		}
	}

	if services != nil {
		for _, svc := range services.Items {
			for _, sp := range svc.Spec.Ports {
				targetPort := ""
				if sp.TargetPort.Type == 0 {
					targetPort = fmt.Sprint(sp.TargetPort.IntVal)
				} else if sp.TargetPort.Type == 1 {
					targetPort = sp.TargetPort.StrVal
				} else {
					targetPort = fmt.Sprint(sp.TargetPort.IntVal)
				}
				payload.ServicePorts = append(payload.ServicePorts, servicePortEntry{
					Service:    svc.Name,
					Type:       string(svc.Spec.Type),
					Port:       sp.Port,
					TargetPort: targetPort,
					NodePort:   sp.NodePort,
					Protocol:   string(sp.Protocol),
					Name:       sp.Name,
				})
			}
		}
	}

	if len(payload.ContainerPorts) == 0 && len(payload.ServicePorts) == 0 {
		return "", errors.New("no ports found")
	}

	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

type appIpPushRequest struct {
	AppIp  string `json:"appIp"`
	Source string `json:"source"`
}

type appPortsPushRequest struct {
	AppPorts string `json:"appPorts"`
	Source   string `json:"source"`
}

func (s *ServerGRPC) pushAppIpToAppo(tenantId, appInsId, appIp string) error {
	if strings.EqualFold(os.Getenv("APPO_APPIP_PUSH_ENABLED"), "false") {
		log.Infof("[AppIPPush] skip by config appInsId=%s", appInsId)
		return nil
	}

	scheme := getEnvOrDefault("APPO_ENDPOINT_SCHEME", "http")
	host := getEnvOrDefault("APPO_ENDPOINT_HOST", "mecm-appo")
	port := getEnvOrDefault("APPO_ENDPOINT_PORT", "8091")
	url := fmt.Sprintf("%s://%s:%s/appo/v1/internal/tenants/%s/app_instance_infos/%s/app_ip",
		scheme, host, port, tenantId, appInsId)

	body, err := json.Marshal(appIpPushRequest{AppIp: appIp, Source: "k8splugin"})
	if err != nil {
		return err
	}

	isTLS := strings.HasPrefix(strings.ToLower(url), "https://")
	client := newAppoPushHTTPClient(isTLS)
	if isTLS {
		log.Infof("[AppIPPush] tls config appInsId=%s tlsServerName=%s insecureSkipVerify=%v",
			appInsId, getEnvOrDefault("APPO_TLS_SERVER_NAME", ""),
			strings.EqualFold(os.Getenv("APPO_TLS_INSECURE_SKIP_VERIFY"), "true"))
	}
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		log.Infof("[AppIPPush] pushing appInsId=%s attempt=%d url=%s", appInsId, attempt, url)
		status, responseBody, sendErr := s.sendAppIpPushRequest(client, url, body)
		if sendErr != nil {
			lastErr = sendErr
			time.Sleep(2 * time.Second)
			continue
		}

		if status >= 200 && status < 300 {
			log.Infof("[AppIPPush] response success appInsId=%s status=%d body=%s",
				appInsId, status, shortTextForLog(responseBody, 200))
			return nil
		}

		if strings.Contains(strings.ToLower(responseBody), "requires tls") && !strings.HasPrefix(url, "https://") {
			secureURL := strings.Replace(url, "http://", "https://", 1)
			secureClient := newAppoPushHTTPClient(true)
			log.Infof("[AppIPPush] endpoint requires TLS, retry with https appInsId=%s secureUrl=%s tlsServerName=%s insecureSkipVerify=%v",
				appInsId, secureURL, getEnvOrDefault("APPO_TLS_SERVER_NAME", ""),
				strings.EqualFold(os.Getenv("APPO_TLS_INSECURE_SKIP_VERIFY"), "true"))
			secureStatus, secureBody, secureErr := s.sendAppIpPushRequest(secureClient, secureURL, body)
			if secureErr == nil && secureStatus >= 200 && secureStatus < 300 {
				log.Infof("[AppIPPush] response success (https fallback) appInsId=%s status=%d body=%s",
					appInsId, secureStatus, shortTextForLog(secureBody, 200))
				return nil
			}
			if secureErr != nil {
				lastErr = secureErr
			} else {
				lastErr = fmt.Errorf("status=%d body=%s", secureStatus, shortTextForLog(secureBody, 200))
			}
			time.Sleep(2 * time.Second)
			continue
		}

		lastErr = fmt.Errorf("status=%d body=%s", status, shortTextForLog(responseBody, 200))
		time.Sleep(2 * time.Second)
	}

	return lastErr
}

func (s *ServerGRPC) pushAppPortsToAppo(tenantId, appInsId, appPorts string) error {
	if strings.EqualFold(os.Getenv("APPO_APPPORTS_PUSH_ENABLED"), "false") {
		log.Infof("[AppPortsPush] skip by config appInsId=%s", appInsId)
		return nil
	}

	scheme := getEnvOrDefault("APPO_ENDPOINT_SCHEME", "http")
	host := getEnvOrDefault("APPO_ENDPOINT_HOST", "mecm-appo")
	port := getEnvOrDefault("APPO_ENDPOINT_PORT", "8091")
	url := fmt.Sprintf("%s://%s:%s/appo/v1/internal/tenants/%s/app_instance_infos/%s/app_ports",
		scheme, host, port, tenantId, appInsId)

	body, err := json.Marshal(appPortsPushRequest{AppPorts: appPorts, Source: "k8splugin"})
	if err != nil {
		return err
	}

	isTLS := strings.HasPrefix(strings.ToLower(url), "https://")
	client := newAppoPushHTTPClient(isTLS)
	if isTLS {
		log.Infof("[AppPortsPush] tls config appInsId=%s tlsServerName=%s insecureSkipVerify=%v",
			appInsId, getEnvOrDefault("APPO_TLS_SERVER_NAME", ""),
			strings.EqualFold(os.Getenv("APPO_TLS_INSECURE_SKIP_VERIFY"), "true"))
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		log.Infof("[AppPortsPush] pushing appInsId=%s attempt=%d url=%s", appInsId, attempt, url)
		status, responseBody, sendErr := s.sendAppIpPushRequest(client, url, body)
		if sendErr != nil {
			lastErr = sendErr
			time.Sleep(2 * time.Second)
			continue
		}

		if status >= 200 && status < 300 {
			log.Infof("[AppPortsPush] response success appInsId=%s status=%d body=%s",
				appInsId, status, shortTextForLog(responseBody, 200))
			return nil
		}

		if strings.Contains(strings.ToLower(responseBody), "requires tls") && !strings.HasPrefix(url, "https://") {
			secureURL := strings.Replace(url, "http://", "https://", 1)
			secureClient := newAppoPushHTTPClient(true)
			log.Infof("[AppPortsPush] endpoint requires TLS, retry with https appInsId=%s secureUrl=%s tlsServerName=%s insecureSkipVerify=%v",
				appInsId, secureURL, getEnvOrDefault("APPO_TLS_SERVER_NAME", ""),
				strings.EqualFold(os.Getenv("APPO_TLS_INSECURE_SKIP_VERIFY"), "true"))
			secureStatus, secureBody, secureErr := s.sendAppIpPushRequest(secureClient, secureURL, body)
			if secureErr == nil && secureStatus >= 200 && secureStatus < 300 {
				log.Infof("[AppPortsPush] response success (https fallback) appInsId=%s status=%d body=%s",
					appInsId, secureStatus, shortTextForLog(secureBody, 200))
				return nil
			}
			if secureErr != nil {
				lastErr = secureErr
			} else {
				lastErr = fmt.Errorf("status=%d body=%s", secureStatus, shortTextForLog(secureBody, 200))
			}
			time.Sleep(2 * time.Second)
			continue
		}

		lastErr = fmt.Errorf("status=%d body=%s", status, shortTextForLog(responseBody, 200))
		time.Sleep(2 * time.Second)
	}

	return lastErr
}

func newAppoPushHTTPClient(isTLS bool) *http.Client {
	if !isTLS {
		return &http.Client{Timeout: 5 * time.Second}
	}

	tlsServerName := strings.TrimSpace(os.Getenv("APPO_TLS_SERVER_NAME"))
	insecureSkipVerify := strings.EqualFold(os.Getenv("APPO_TLS_INSECURE_SKIP_VERIFY"), "true")

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			ServerName:         tlsServerName,
			InsecureSkipVerify: insecureSkipVerify,
		},
	}

	return &http.Client{
		Timeout:   5 * time.Second,
		Transport: transport,
	}
}

func (s *ServerGRPC) sendAppIpPushRequest(client *http.Client, url string, body []byte) (int, string, error) {
	req, reqErr := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if reqErr != nil {
		return 0, "", reqErr
	}
	req.Header.Set("Content-Type", "application/json")

	resp, doErr := client.Do(req)
	if doErr != nil {
		return 0, "", doErr
	}

	respBody, _ := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, string(respBody), nil
}

func getEnvOrDefault(key, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

type networkStatusEntry struct {
	Name    string   `json:"name"`
	IPs     []string `json:"ips"`
	Default bool     `json:"default"`
}

func (s *ServerGRPC) getAppIpByNetworkPlane(tenantId, hostIp, namespace, releaseName, networkPlane string) (string, error) {
	kubeConfigPath := KubeconfigPath + tenantId + "/" + hostIp
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return "", err
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return "", err
	}

	pods, err := s.getReleasePods(clientset, namespace, releaseName)
	if err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", errors.New("pod not found for release")
	}
	log.Infof("[AppIP] candidate pods releaseName=%s namespace=%s count=%d", releaseName, namespace, len(pods.Items))
	for _, pod := range pods.Items {
		log.Infof("[AppIP] pod=%s podIP=%s labels=%s", pod.Name, pod.Status.PodIP, shortTextForLog(mapToCompactJSON(pod.Labels), 300))
	}

	targetNetwork := networkPlane

	// Try to infer target network from pod annotation when request parameter is missing/default.
	if targetNetwork == "" || targetNetwork == util.Default {
		for _, pod := range pods.Items {
			if networkAnnotation, found := pod.Annotations["k8s.v1.cni.cncf.io/networks"]; found {
				log.Infof("[AppIP] pod=%s networks-annotation=%s", pod.Name, shortTextForLog(networkAnnotation, 300))
				annotationNetwork := parseNetworkNameFromNetworksAnnotation(networkAnnotation)
				if annotationNetwork != "" && annotationNetwork != util.Default {
					targetNetwork = annotationNetwork
					break
				}
			}
		}
	}
	log.Infof("[AppIP] resolved targetNetwork=%s requestNetworkPlane=%s releaseName=%s", targetNetwork, networkPlane, releaseName)

	hasSpecificTarget := targetNetwork != "" && targetNetwork != util.Default

	// First pass: select IP by exact target network match.
	for _, pod := range pods.Items {
		networkStatus, ok := pod.Annotations["k8s.v1.cni.cncf.io/network-status"]
		if ok && hasSpecificTarget {
			log.Infof("[AppIP] specific-target pod=%s network-status=%s", pod.Name, shortTextForLog(networkStatus, 500))
			ip := selectIPFromNetworkStatus(networkStatus, targetNetwork)
			if ip != "" {
				log.Infof("[AppIP] selected by specific-target pod=%s targetNetwork=%s ip=%s", pod.Name, targetNetwork, ip)
				return ip, nil
			}
		}
	}

	// If a specific network is requested, do not fallback to default/PodIP.
	// Keep polling until target network IP is present in network-status.
	if hasSpecificTarget {
		return "", errors.New("target network ip not ready")
	}

	// Second pass: fallback to default network in network-status.
	// If request doesn't carry explicit network plane but pod has multus network entries,
	// prefer non-default network IP first.
	for _, pod := range pods.Items {
		networkStatus, ok := pod.Annotations["k8s.v1.cni.cncf.io/network-status"]
		if ok {
			log.Infof("[AppIP] non-default fallback pod=%s network-status=%s", pod.Name, shortTextForLog(networkStatus, 500))
			ip := selectPreferredNonDefaultIPFromNetworkStatus(networkStatus)
			if ip != "" {
				log.Infof("[AppIP] selected by non-default fallback pod=%s ip=%s", pod.Name, ip)
				return ip, nil
			}
		}
	}

	// Third pass: fallback to default network in network-status.
	for _, pod := range pods.Items {
		networkStatus, ok := pod.Annotations["k8s.v1.cni.cncf.io/network-status"]
		if ok {
			ip := selectIPFromNetworkStatus(networkStatus, util.Default)
			if ip != "" {
				log.Infof("[AppIP] selected by default fallback pod=%s ip=%s", pod.Name, ip)
				return ip, nil
			}
		}
	}

	// Fourth pass: fallback to PodIP.
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" {
			log.Infof("[AppIP] selected by podIP fallback pod=%s ip=%s", pod.Name, pod.Status.PodIP)
			return pod.Status.PodIP, nil
		}
	}

	return "", errors.New("app ip not found")
}

func (s *ServerGRPC) getReleasePods(clientset *kubernetes.Clientset, namespace, releaseName string) (*v1.PodList, error) {
	options := metav1.ListOptions{LabelSelector: "app.kubernetes.io/instance=" + releaseName}
	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), options)
	if err != nil {
		return nil, err
	}
	if len(pods.Items) > 0 {
		return pods, nil
	}

	appName := extractAppNameFromRelease(releaseName)
	if appName != "" {
		options = metav1.ListOptions{LabelSelector: "app=" + appName}
		pods, err = clientset.CoreV1().Pods(namespace).List(context.Background(), options)
		if err != nil {
			return nil, err
		}
		if len(pods.Items) > 0 {
			return pods, nil
		}
	}

	options = metav1.ListOptions{LabelSelector: "release=" + releaseName}
	pods, err = clientset.CoreV1().Pods(namespace).List(context.Background(), options)
	if err != nil {
		return nil, err
	}
	if len(pods.Items) > 0 {
		return pods, nil
	}

	allPods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	filtered := &v1.PodList{}
	for _, pod := range allPods.Items {
		if strings.Contains(pod.Name, releaseName) {
			filtered.Items = append(filtered.Items, pod)
			continue
		}
		if appName != "" && strings.Contains(pod.Name, appName+"-") {
			filtered.Items = append(filtered.Items, pod)
		}
	}
	if len(filtered.Items) > 0 {
		return filtered, nil
	}

	// Last fallback: return all pods in namespace and let IP selection logic decide safely.
	return allPods, nil
}

func (s *ServerGRPC) getReleaseServices(clientset *kubernetes.Clientset, namespace, releaseName string) (*v1.ServiceList, error) {
	options := metav1.ListOptions{LabelSelector: "app.kubernetes.io/instance=" + releaseName}
	services, err := clientset.CoreV1().Services(namespace).List(context.Background(), options)
	if err != nil {
		return nil, err
	}
	if len(services.Items) > 0 {
		return services, nil
	}

	appName := extractAppNameFromRelease(releaseName)
	if appName != "" {
		options = metav1.ListOptions{LabelSelector: "app=" + appName}
		services, err = clientset.CoreV1().Services(namespace).List(context.Background(), options)
		if err != nil {
			return nil, err
		}
		if len(services.Items) > 0 {
			return services, nil
		}
	}

	options = metav1.ListOptions{LabelSelector: "release=" + releaseName}
	services, err = clientset.CoreV1().Services(namespace).List(context.Background(), options)
	if err != nil {
		return nil, err
	}
	if len(services.Items) > 0 {
		return services, nil
	}

	allServices, err := clientset.CoreV1().Services(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	filtered := &v1.ServiceList{}
	for _, svc := range allServices.Items {
		if strings.Contains(svc.Name, releaseName) {
			filtered.Items = append(filtered.Items, svc)
			continue
		}
		if appName != "" && strings.Contains(svc.Name, appName+"-") {
			filtered.Items = append(filtered.Items, svc)
		}
	}
	return filtered, nil
}

func selectIPFromNetworkStatus(networkStatus, networkPlane string) string {
	var entries []networkStatusEntry
	if err := json.Unmarshal([]byte(networkStatus), &entries); err != nil {
		return ""
	}

	if networkPlane != "" && networkPlane != util.Default {
		target := normalizeNetworkName(networkPlane)
		for _, item := range entries {
			if len(item.IPs) == 0 {
				continue
			}
			if normalizeNetworkName(item.Name) == target {
				return item.IPs[0]
			}
		}
		return ""
	}

	for _, item := range entries {
		if len(item.IPs) == 0 {
			continue
		}
		if item.Default {
			return item.IPs[0]
		}
	}

	for _, item := range entries {
		if len(item.IPs) > 0 {
			return item.IPs[0]
		}
	}

	return ""
}

func selectPreferredNonDefaultIPFromNetworkStatus(networkStatus string) string {
	var entries []networkStatusEntry
	if err := json.Unmarshal([]byte(networkStatus), &entries); err != nil {
		return ""
	}

	for _, item := range entries {
		if len(item.IPs) == 0 {
			continue
		}
		if item.Default {
			continue
		}
		if normalizeNetworkName(item.Name) != "" {
			return item.IPs[0]
		}
	}

	return ""
}

func parseNetworkNameFromNetworksAnnotation(networksAnnotation string) string {
	annotation := strings.TrimSpace(networksAnnotation)
	if annotation == "" {
		return ""
	}

	if strings.HasPrefix(annotation, "[") {
		var networkObjects []map[string]interface{}
		if err := json.Unmarshal([]byte(annotation), &networkObjects); err == nil {
			for _, item := range networkObjects {
				if nameRaw, ok := item["name"]; ok {
					if name, ok := nameRaw.(string); ok {
						parsed := normalizeNetworkName(name)
						if parsed != "" {
							return parsed
						}
					}
				}
			}
		}

		var networkList []string
		if err := json.Unmarshal([]byte(annotation), &networkList); err == nil {
			for _, item := range networkList {
				parsed := normalizeNetworkName(item)
				if parsed != "" {
					return parsed
				}
			}
		}
	}

	first := strings.Split(annotation, ",")[0]
	return normalizeNetworkName(first)
}

func normalizeNetworkName(networkName string) string {
	name := strings.TrimSpace(networkName)
	if name == "" {
		return ""
	}
	if atIndex := strings.Index(name, "@"); atIndex > 0 {
		name = name[:atIndex]
	}
	if slashIndex := strings.LastIndex(name, "/"); slashIndex >= 0 && slashIndex < len(name)-1 {
		name = name[slashIndex+1:]
	}
	return strings.TrimSpace(name)
}

func extractAppNameFromRelease(releaseName string) string {
	name := strings.TrimSpace(releaseName)
	if name == "" {
		return ""
	}
	re := regexp.MustCompile(`^(.*)-[0-9]{8}$`)
	parts := re.FindStringSubmatch(name)
	if len(parts) == 2 && parts[1] != "" {
		return parts[1]
	}
	return name
}

func shortTextForLog(text string, limit int) string {
	if limit <= 0 {
		return text
	}
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

func mapToCompactJSON(values map[string]string) string {
	if len(values) == 0 {
		return "{}"
	}
	bytes, err := json.Marshal(values)
	if err != nil {
		return "{marshal-error}"
	}
	return string(bytes)
}

// Upload file configuration
func (s *ServerGRPC) UploadConfig(stream internal_lcmservice.AppLCM_UploadConfigServer) (err error) {
	var res internal_lcmservice.UploadCfgResponse
	res.Status = util.Failure

	ctx := stream.Context()
	err = s.displayReceivedMsg(ctx, util.UploadConfig)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, util.FailedToDispRecvMsg)
		sendUploadCfgResponse(stream, &res)
		return err
	}

	hostIp, tenantId, err := s.validateInputParamsForUploadCfg(stream)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, util.FailedToValInputParams)
		sendUploadCfgResponse(stream, &res)
		return
	}

	file, err := s.getUploadConfigFile(stream)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to get upload config file")
		sendUploadCfgResponse(stream, &res)
		return err
	}

	if !util.CreateDir(KubeconfigPath + tenantId) {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to create config directory")
		sendUploadCfgResponse(stream, &res)
		return err
	}

	configPath := KubeconfigPath + tenantId + "/" + hostIp
	newFile, err := os.Create(configPath)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to create config path")
		sendUploadCfgResponse(stream, &res)
		return err
	}

	if len(file.Bytes()) > util.MaxConfigFile {
		s.displayResponseMsg(ctx, util.UploadConfig, "file size is larger than max size")
		sendUploadCfgResponse(stream, &res)
		return err
	}

	defer newFile.Close()
	_, err = newFile.Write(file.Bytes())
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "config IO operation error")
		sendUploadCfgResponse(stream, &res)
		return err
	}

	res.Status = util.Success
	sendUploadCfgResponse(stream, &res)
	s.handleLoggingForSuccess(ctx, util.UploadConfig, "Upload config is successful")
	return nil
}

// Remove file configuration
func (s *ServerGRPC) RemoveConfig(ctx context.Context,
	request *internal_lcmservice.RemoveCfgRequest) (*internal_lcmservice.RemoveCfgResponse, error) {

	resp := &internal_lcmservice.RemoveCfgResponse{
		Status: util.Failure,
	}

	err := s.displayReceivedMsg(ctx, util.RemoveConfig)
	if err != nil {
		s.displayResponseMsg(ctx, util.RemoveConfig, util.FailedToDispRecvMsg)
		return resp, err
	}

	hostIp, tenantId, err := s.validateInputParamsForRemoveCfg(request)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, util.FailedToValInputParams)
		return resp, err
	}
	//
	//hostIp, err := s.validateInputParamsForRemoveCfg(request)
	//if err != nil {
	//	s.displayResponseMsg(ctx, util.RemoveConfig, util.FailedToValInputParams)
	//	return resp, err
	//}
	configPath := KubeconfigPath + tenantId + "/" + hostIp
	err = os.Remove(configPath)
	if err != nil {
		log.Error("failed to remove host config file")
		s.displayResponseMsg(ctx, util.RemoveConfig, "failed to remove host config file")
		return resp, err
	}

	resp = &internal_lcmservice.RemoveCfgResponse{
		Status: util.Success,
	}
	s.handleLoggingForSuccess(ctx, util.UploadConfig, "Remove config is successful")
	return resp, nil
}

// Validate input parameters for remove config
func (s *ServerGRPC) validateInputParamsForRemoveCfg(request *internal_lcmservice.RemoveCfgRequest) (hostIp string,
	tenantId string,err error) {
	accessToken := request.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmAdminRole})
	if err != nil {
		if err.Error() == util.Forbidden {
			return "", "", s.logError(status.Error(codes.PermissionDenied, util.Forbidden))
		} else {
			return "", "", s.logError(status.Error(codes.InvalidArgument, util.AccssTokenIsInvalid))
		}
	}
	hostIp = request.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "","", s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}

	tenantId = request.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "","", s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}
	return hostIp, tenantId, nil
}

// Context Error
func (s *ServerGRPC) contextError(ctx context.Context) error {
	switch ctx.Err() {
	case context.Canceled:
		return s.logError(status.Error(codes.Canceled, "request is canceled"))
	case context.DeadlineExceeded:
		return s.logError(status.Error(codes.DeadlineExceeded, "deadline is exceeded"))
	default:
		return nil
	}
}

// Logging error
func (s *ServerGRPC) logError(err error) error {
	if err != nil {
		log.Errorf("Error Information: %v", err)
	}
	return err
}

// Validate input parameters for termination
func (s *ServerGRPC) validateInputParamsForTerm(
	req *internal_lcmservice.TerminateRequest) (tenantId string, hostIp string, appInsId string, err error) {
	accessToken := req.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmAdminRole})
	if err != nil {
		if err.Error() == util.Forbidden {
			return "", "", "", s.logError(status.Error(codes.PermissionDenied, util.Forbidden))
		} else {
			return "", "", "", s.logError(status.Error(codes.InvalidArgument,
				util.AccssTokenIsInvalid))
		}
	}

	hostIp = req.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument,
			util.HostIpIsInvalid))
	}

	appInsId = req.GetAppInstanceId()
	err = util.ValidateUUID(appInsId)
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument,
			util.AppInsIdValid))
	}
	tenantId = req.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "","", "", s.logError(status.Error(codes.InvalidArgument, util.TenantIdIsInvalid))
	}
	return tenantId, hostIp, appInsId, nil
}

// Validate input parameters for termination
func (s *ServerGRPC) validateInputParamsForInstantiate(
	req *internal_lcmservice.InstantiateRequest) (tenantId string, packageId string, hostIp string, appInsId string, ak string, sk string, err error) {
	accessToken := req.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmAdminRole})
	if err != nil {
		if err.Error() == util.Forbidden {
			return "", "", "", "",  "", "", s.logError(status.Error(codes.PermissionDenied, util.Forbidden))
		} else {
			return "", "", "", "",  "", "", s.logError(status.Error(codes.InvalidArgument,
				util.AccssTokenIsInvalid))
		}
	}

	hostIp = req.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "", "", "", "",  "", "", s.logError(status.Error(codes.InvalidArgument,
			util.HostIpIsInvalid))
	}

	packageId = req.GetAppPackageId()
	if packageId == "" {
		return "", "", "", "",  "", "", s.logError(status.Error(codes.InvalidArgument,
			util.PackageIdIsInvalid))
	}

	tenantId = req.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "", "", "", "",  "", "", s.logError(status.Error(codes.InvalidArgument,
			util.TenantIdIsInvalid))
	}

	parameters := req.GetParameters()
	ak = parameters["ak"]
	if ak == "" {
    		return "", "", "", "",  "", "", s.logError(status.Error(codes.InvalidArgument,
    			util.AKIsInvalid))
    	}
	sk = parameters["sk"]
    if sk == "" {
    		return "", "", "", "",  "", "", s.logError(status.Error(codes.InvalidArgument,
    			util.SKIsInvalid))
    	}


	appInsId = req.GetAppInstanceId()
	err = util.ValidateUUID(appInsId)
	if err != nil {
		return "", "", "", "", "", "", s.logError(status.Error(codes.InvalidArgument,
			util.AppInsIdValid))
	}

	return tenantId, packageId, hostIp, appInsId, ak, sk, nil
}

// Validate input parameters for upload configuration
func (s *ServerGRPC) validateInputParamsForUploadCfg(
	stream internal_lcmservice.AppLCM_UploadConfigServer) (hostIp string, tenantId string, err error) {
	// Receive metadata which is accesstoken
	req, err := stream.Recv()
	if err != nil {
		log.Error(util.CannotReceivePackage)
		return
	}
	accessToken := req.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmAdminRole})
	if err != nil {
		if err.Error() == util.Forbidden {
			return "", "", s.logError(status.Error(codes.PermissionDenied, util.Forbidden))
		} else {
			return "", "", s.logError(status.Error(codes.InvalidArgument, util.AccssTokenIsInvalid))
		}
	}

	// Receive metadata which is host ip
	req, err = stream.Recv()
	if err != nil {
		log.Error(util.CannotReceivePackage)
		return
	}

	tenantId = req.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "","", s.logError(status.Error(codes.InvalidArgument, util.TenantIdIsInvalid))
	}

	// Receive metadata which is host ip
	req, err = stream.Recv()
	if err != nil {
		log.Error(util.CannotReceivePackage)
		return
	}
	hostIp = req.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "","", s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}


	return hostIp, tenantId, nil
}

// Validate input parameters for pod describe
func (s *ServerGRPC) validateInputParamsForPodDesc(
	req *internal_lcmservice.WorkloadEventsRequest) (hostIp string, podName string,tenantId string, err error) {

	accessToken := req.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmAdminRole, util.MecmGuestRole})
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument,
			util.AccssTokenIsInvalid))
	}

	hostIp = req.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}

	appInsId := req.GetAppInstanceId()
	err = util.ValidateUUID(appInsId)
	if err != nil {
		return "", "","", s.logError(status.Error(codes.InvalidArgument, util.AppInsIdValid))
	}

	tenantId = req.GetTenantId()
	err = util.ValidateUUID(appInsId)
	if err != nil {
		return "", "","", s.logError(status.Error(codes.InvalidArgument, util.TenantIdIsInvalid))
	}
	return hostIp, appInsId, tenantId, nil
}

// Validate input parameters for Query
func (s *ServerGRPC) validateInputParamsForQuery(
	req *internal_lcmservice.QueryRequest) (tenantId string, hostIp string, appInsId string, err error) {

	accessToken := req.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmGuestRole, util.MecmAdminRole})
	if err != nil {
		return "", "","" , s.logError(status.Error(codes.InvalidArgument,
			util.AccssTokenIsInvalid))
	}

	hostIp = req.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}

	appInsId = req.GetAppInstanceId()
	err = util.ValidateUUID(appInsId)
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument, util.AppInsIdValid))
	}
	tenantId = req.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument, util.TenantIdIsInvalid))
	}

	return tenantId, hostIp, appInsId, nil
}

// Validate input parameters for Query kpi
func (s *ServerGRPC) validateInputParamsForQueryKPI(
	req *internal_lcmservice.QueryKPIRequest) (tenantId string, hostIp string, err error) {

	accessToken := req.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmGuestRole, util.MecmAdminRole})
	if err != nil {
		return "", "", s.logError(status.Error(codes.InvalidArgument,
			util.AccssTokenIsInvalid))
	}

	hostIp = req.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "", "", s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}
	tenantId = req.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "", "", s.logError(status.Error(codes.InvalidArgument, util.TenantIdIsInvalid))
	}
	return tenantId, hostIp, nil
}

// Get upload configuration file
func (s *ServerGRPC) getUploadConfigFile(stream internal_lcmservice.AppLCM_UploadConfigServer) (but bytes.Buffer, err error) {
	// Receive upload config file
	file := bytes.Buffer{}
	for {
		err := s.contextError(stream.Context())
		if err != nil {
			return file, err
		}

		log.Debug("Waiting to receive more data")

		req, err := stream.Recv()
		if err == io.EOF {
			log.Debug("No more data")
			break
		}
		if err != nil {
			return file, s.logError(status.Error(codes.Unknown, "cannot receive chunk data"))
		}

		// Receive chunk and write to package
		chunk := req.GetConfigFile()

		_, err = file.Write(chunk)
		if err != nil {
			return file, s.logError(status.Error(codes.Internal, "cannot write chunk data"))
		}
	}
	return file, nil
}

// Insert or update application instance record
func (s *ServerGRPC) insertOrUpdateAppInsRecord(appInsId, hostIp, releaseName, namespace, appIp string) (err error) {
	appInfoRecord := &models.AppInstanceInfo{
		AppInsId:   appInsId,
		HostIp:     hostIp,
		WorkloadId: releaseName,
		Namespace:  namespace,
		AppIp:      appIp,
	}
	err = s.db.InsertOrUpdateData(appInfoRecord, util.AppInsId)
	if err != nil && err.Error() != "LastInsertId is not supported by this driver" {
		return s.logError(status.Error(codes.InvalidArgument,
			"failed to save app info record to database."))
	}
	return nil
}

// Insert or update application package record
func (s *ServerGRPC) insertOrUpdateAppPkgRecord(packageId string, tenantId string,
	hostIp string, dockerImages string) (err error) {
	appPkgRecord := &models.AppPackage{
		AppPkgId:     packageId + tenantId + hostIp,
		HostIp:       hostIp,
		TenantId:     tenantId,
		DockerImages: dockerImages,
		PackageId: packageId,
	}
	err = s.db.InsertOrUpdateData(appPkgRecord, util.AppPkgId)
	if err != nil && err.Error() != "LastInsertId is not supported by this driver" {
		return s.logError(status.Error(codes.InvalidArgument,
			"failed to save app info record to database."))
	}
	return nil
}

// Delete app instance record
func (s *ServerGRPC) deleteAppInfoRecord(appInsId string) error {
	appInfoRecord := &models.AppInstanceInfo{
		AppInsId: appInsId,
	}

	err := s.db.DeleteData(appInfoRecord, util.AppInsId)
	if err != nil {
		return s.logError(status.Error(codes.InvalidArgument,
			"failed to delete app info record from database"))
	}
	return nil
}

// Delete app instance record
func (s *ServerGRPC) deleteAppPackageRecord(appPkgId, tenantId, hostIp string) error {
	appPkgRecord := &models.AppPackage{
		AppPkgId: appPkgId + tenantId + hostIp,
	}

	err := s.db.DeleteData(appPkgRecord, util.AppPkgId)
	if err != nil {
		return s.logError(status.Error(codes.InvalidArgument,
			"failed to delete app package record from database"))
	}
	return nil
}

// display response message
func (s *ServerGRPC) handleLoggingForSuccess(ctx context.Context, rpcName string, msg string) {
	clientIp, err := s.getClientAddress(ctx)
	if err != nil {
		return
	}

	log.Info("Response message for ClientIP [" + clientIp + "]" +
		util.RpcName + rpcName + "] Result [Success: " + msg + ".]")
}

// Display received message
func (s *ServerGRPC) displayReceivedMsg(ctx context.Context, rpcName string) error {
	clientIp, err := s.getClientAddress(ctx)
	if err != nil {
		return err
	}

	log.Info("Received message from ClientIP [" + clientIp + "]" + util.RpcName + rpcName + "]")
	return nil
}

// display response message
func (s *ServerGRPC) displayResponseMsg(ctx context.Context, rpcName string, errMsg string) {
	clientIp, err := s.getClientAddress(ctx)
	if err != nil {
		return
	}

	log.Info("Response message for ClientIP [" + clientIp + "]" +
		util.RpcName + rpcName + "] Result [Failure: " + errMsg + ".]")
}

// Get client address
func (s *ServerGRPC) getClientAddress(ctx context.Context) (remoteIp string, err error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "",  s.logError(status.Errorf(codes.NotFound, "failed to get peer from ctx"))
	}
	if pr.Addr == net.Addr(nil) {
		return "",  s.logError(status.Errorf(codes.NotFound, "failed to get peer address"))
	}
	clientAddr := pr.Addr.String()
	clientIp := strings.Split(clientAddr, ":")
	return clientIp[0], nil
}

// Send upload config response
func sendUploadCfgResponse(stream internal_lcmservice.AppLCM_UploadConfigServer,
	res *internal_lcmservice.UploadCfgResponse) {
	err := stream.SendAndClose(res)
	if err != nil {
		log.Errorf("cannot send response: %v", err)
		return
	}
}

// Upload file configuration
func (s *ServerGRPC) UploadPackage(stream internal_lcmservice.AppLCM_UploadPackageServer) (err error) {
	var res internal_lcmservice.UploadPackageResponse
	res.Status = util.Failure

	ctx := stream.Context()
	err = s.displayReceivedMsg(ctx, util.UploadPackage)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadPackage, util.FailedToDispRecvMsg)
		sendUploadPackageResponse(stream, &res)
		return err
	}

	hostIp, tenantId, packageId, err := s.validateInputParamsForUploadPackage(stream)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, util.FailedToValInputParams)
		sendUploadPackageResponse(stream, &res)
		return
	}

	file, err := s.getUploadPackageFile(stream)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to get upload package file")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	if !util.CreateDir(appPackagesBasePath + tenantId) {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to create package directory")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	packagePath := appPackagesBasePath + tenantId + "/" + packageId + hostIp
	if !util.CreateDir(packagePath) {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to create config directory")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	packageFilePath := packagePath + "/" + packageId + ".csar"
	newFile, err := os.Create(packageFilePath)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to create application package path")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	if len(file.Bytes()) > util.MaxPackageFile {
		s.displayResponseMsg(ctx, util.UploadConfig, "package size is larger than max size")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	defer newFile.Close()
	_, err = newFile.Write(file.Bytes())
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "package IO operation error")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	packagePath, err = s.extractCsarPackage(packageFilePath)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to extract csar app package")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	dockerImages, err := s.loadDockerImagesToHost(packagePath)
	if err != nil {
		s.displayResponseMsg(ctx, util.UploadConfig, "failed to process SwImageDescr")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	err = s.insertOrUpdateAppPkgRecord(packageId, tenantId, hostIp, dockerImages)
	if err != nil {
		s.displayResponseMsg(ctx, util.Instantiate, "failed to insert or update app package record")
		sendUploadPackageResponse(stream, &res)
		return err
	}

	res.Status = util.Success
	sendUploadPackageResponse(stream, &res)
	s.handleLoggingForSuccess(ctx, util.UploadConfig, "Uploaded package successfully")
	return nil
}

// Send upload config response
func sendUploadPackageResponse(stream internal_lcmservice.AppLCM_UploadPackageServer,
	res *internal_lcmservice.UploadPackageResponse) {
	err := stream.SendAndClose(res)
	if err != nil {
		log.Errorf("cannot send response: %v", err)
		return
	}
}

// Validate input parameters for upload configuration
func (s *ServerGRPC) validateInputParamsForUploadPackage(
	stream internal_lcmservice.AppLCM_UploadPackageServer) (hostIp, tenantId, packageId string, err error) {
	// Receive metadata which is accesstoken
	req, err := stream.Recv()
	if err != nil {
		log.Error(util.CannotReceivePackage)
		return
	}
	accessToken := req.GetAccessToken()
	err = util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmAdminRole})
	if err != nil {
		if err.Error() == util.Forbidden {
			return "", "", "",  s.logError(status.Error(codes.PermissionDenied, util.Forbidden))
		} else {
			return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.AccssTokenIsInvalid))
		}
	}

	// Receive metadata which is package ID
	req, err = stream.Recv()
	if err != nil {
		log.Error(util.CannotReceivePackage)
		return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.CannotReceivePackage))
	}

	packageId = req.GetAppPackageId()
	if packageId == "" {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument, util.PackageIdIsInvalid))
	}

	// Receive metadata which is host ip
	req, err = stream.Recv()
	if err != nil {
		log.Error(util.CannotReceivePackage)
		return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.CannotReceivePackage))
	}

	hostIp = req.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}

	// Receive metadata which is tenant ID
	req, err = stream.Recv()
	if err != nil {
		log.Error(util.CannotReceivePackage)
		return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.CannotReceivePackage))
	}
	tenantId = req.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.TenantIsInvalid))
	}

	return hostIp, tenantId, packageId, nil
}

// Get upload package file
func (s *ServerGRPC) getUploadPackageFile(stream internal_lcmservice.AppLCM_UploadPackageServer) (but bytes.Buffer, err error) {
	// Receive upload package file
	file := bytes.Buffer{}
	for {
		err := s.contextError(stream.Context())
		if err != nil {
			return file, err
		}

		log.Debug("Waiting to receive more data")

		req, err := stream.Recv()
		if err == io.EOF {
			log.Debug("No more data")
			break
		}
		if err != nil {
			return file, s.logError(status.Error(codes.Unknown, "cannot receive chunk data"))
		}

		// Receive chunk and write to package
		chunk := req.GetPackage()

		_, err = file.Write(chunk)
		if err != nil {
			return file, s.logError(status.Error(codes.Internal, "cannot write chunk data"))
		}
	}
	return file, nil
}

// Delete application package
func (s *ServerGRPC) DeletePackage(ctx context.Context,
	request *internal_lcmservice.DeletePackageRequest) (*internal_lcmservice.DeletePackageResponse, error) {

	resp := &internal_lcmservice.DeletePackageResponse{
		Status: util.Failure,
	}

	err := s.displayReceivedMsg(ctx, util.DeletePackage)
	if err != nil {
		s.displayResponseMsg(ctx, util.DeletePackage, util.FailedToDispRecvMsg)
		return resp, err
	}

	//tenantId, hostIp, packageId, err := s.validateInputParamsForDeletePackage(request)
	tenantId, hostIp, packageId, err := s.validateInputParamsForDeletePackage(request)
	if err != nil {
		s.displayResponseMsg(ctx, util.DeletePackage, util.FailedToValInputParams)
		return resp, err
	}

	appPkgRecord, err := s.getAppPackageRecord(hostIp, packageId, tenantId)
	if err != nil {
		log.Error(util.AppPkgRecordDoesNotExit)
		s.displayResponseMsg(ctx, util.DeletePackage, util.AppPkgRecordDoesNotExit)
		return resp, err
	}

	err = s.deleteAppPackageRecord(packageId, tenantId, hostIp)
	if err != nil {
		s.displayResponseMsg(ctx, util.Terminate, "failed to delete app package record from database")
		return resp, err
	}

	_ = s.deleteDockerImagesFromHost(appPkgRecord.DockerImages)

	packagePath := appPackagesBasePath + tenantId + "/" + packageId + appPkgRecord.HostIp
	err = s.deletePackage(packagePath)
	if err != nil {
		log.Error("failed to delete application package file")
		s.displayResponseMsg(ctx, util.DeletePackage, util.FailedToDelAppPkg)
		return resp, nil
	}

	resp = &internal_lcmservice.DeletePackageResponse{
		Status: util.Success,
	}
	s.handleLoggingForSuccess(ctx, util.DeletePackage, "Deleted application package successfully")
	return resp, nil
}

// Upload package status
func (s *ServerGRPC) QueryPackageStatus(ctx context.Context,
	request *internal_lcmservice.QueryPackageStatusRequest) (*internal_lcmservice.QueryPackageStatusResponse, error) {

	resp := &internal_lcmservice.QueryPackageStatusResponse{
		Response: "Distributed",
	}

	err := s.displayReceivedMsg(ctx, util.QueryPackageStatus)
	if err != nil {
		s.displayResponseMsg(ctx, util.QueryPackageStatus, util.FailedToDispRecvMsg)
		return resp, err
	}

	s.handleLoggingForSuccess(ctx, util.QueryPackageStatus, "Return upload package status successfully")
	return resp, nil
}


func (s *ServerGRPC) deletePackage(appPkgPath string) error {

	tenantPath := path.Dir(appPkgPath)

	//remove package directory
	err := os.RemoveAll(appPkgPath)
	if err != nil {
		return errors.New("failed to delete application package file")
	}

	tenantDir, err := os.Open(tenantPath)
	if err != nil {
		return errors.New(util.FailedToDelAppPkg)
	}
	defer tenantDir.Close()

	_, err = tenantDir.Readdir(1)

	if err == io.EOF {
		err := os.Remove(tenantPath)
		if err != nil {
			return errors.New(util.FailedToDelAppPkg)
		}
		return nil
	}
	return nil
}

// Validate input parameters for remove config
func (s *ServerGRPC) validateInputParamsForDeletePackage(request *internal_lcmservice.DeletePackageRequest) (string,
	string, string, error) {
	accessToken := request.GetAccessToken()
	err := util.ValidateAccessToken(accessToken, []string{util.MecmTenantRole, util.MecmAdminRole})
	if err != nil {
		if err.Error() == util.Forbidden {
			return "", "", "",  s.logError(status.Error(codes.PermissionDenied, util.Forbidden))
		} else {
			return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.AccssTokenIsInvalid))
		}
	}
	hostIp := request.GetHostIp()
	err = util.ValidateIpv4Address(hostIp)
	if err != nil {
		return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}

	packageId := request.GetAppPackageId()
	if packageId == "" {
		return "", "", "",  s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}

	tenantId := request.GetTenantId()
	err = util.ValidateUUID(tenantId)
	if err != nil {
		return "", "", "", s.logError(status.Error(codes.InvalidArgument, util.HostIpIsInvalid))
	}
	return tenantId, hostIp, packageId, nil
}

// extract CSAR package
func (c *ServerGRPC) extractCsarPackage(packagePath string) (string, error) {
	zipReader, _ := zip.OpenReader(packagePath)
	if len(zipReader.File) > util.TooManyFile {
		return "", errors.New("Too many files contains in zip file")
	}
	defer zipReader.Close()
	var totalWrote int64
	packageDir := path.Dir(packagePath)
	err := os.MkdirAll(packageDir, 0750)
	if err != nil {
		log.Error("Failed to make directory")
		return "" ,errors.New("Failed to make directory")
	}
	for _, file := range zipReader.Reader.File {

		zippedFile, err := file.Open()
		if err != nil || zippedFile == nil {
			log.Error("Failed to open zip file")
			continue
		}
		if file.UncompressedSize64 > util.SingleFileTooBig || totalWrote > util.TooBig {
			log.Error("File size limit is exceeded")
		}

		defer zippedFile.Close()

		isContinue, wrote := c.extractFiles(file, zippedFile, totalWrote, packageDir)
		if isContinue {
			continue
		}
		totalWrote = wrote
	}
	return packageDir, nil
}

// Extract files
func (c *ServerGRPC) extractFiles(file *zip.File, zippedFile io.ReadCloser, totalWrote int64, dirName string) (bool, int64) {
	targetDir := dirName
	extractedFilePath := filepath.Join(
		targetDir,
		file.Name,
	)

	if file.FileInfo().IsDir() {
		err := os.MkdirAll(extractedFilePath, 0750)
		if err != nil {
			log.Error("Failed to create directory")
		}
	} else {
		parent := filepath.Dir(extractedFilePath)
		if _, err := os.Stat(parent); os.IsNotExist(err) {
			_ = os.MkdirAll(parent, 0750)
		}
		outputFile, err := os.OpenFile(
			extractedFilePath,
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
			0750,
		)
		if err != nil || outputFile == nil {
			log.Error("The output file is nil")
			return true, totalWrote
		}

		defer outputFile.Close()

		wt, err := io.Copy(outputFile, zippedFile)
		if err != nil {
			log.Error("Failed to copy zipped file")
		}
		totalWrote += wt
	}
	return false, totalWrote
}

func (s *ServerGRPC) deleteDockerImagesFromHost(dockerImages string) error {
	log.Info("Delete docker images")
	dockers := strings.Split(dockerImages, ",")
	for i := range dockers {
		log.WithFields(log.Fields{
			"delete docker image": dockers[i],
		}).Info("delete docker images")

		//delete docker images form host machine using docker client
	}
	return nil
}

// get sw image descriptors
func (c *ServerGRPC) loadDockerImagesToHost(packagePath string) (string, error) {

	var imageDescriptors []models.SwImageDescriptor

	jsonFile, err := os.Open(packagePath + "/Image/SwImageDesc.json")
	if err != nil {
		return "", errors.New("failed to get SwImageDesc.json")
	}
	defer jsonFile.Close()

	imageDescrBytes, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(imageDescrBytes, &imageDescriptors)

	dockerImages := make([]string, 0)
	for i := range imageDescriptors {
		log.WithFields(log.Fields{
			"loading docker image": imageDescriptors[i].SwImage,
		}).Info("load docker images")

		dockerImages = append(dockerImages, imageDescriptors[i].SwImage)

		//load docker image to docker host using docker client
	}

	return strings.Join(dockerImages,", "), nil
}

// Get app package record
func (c *ServerGRPC) getAppPackageRecord(hostIp, appPkgId, tenantId string) (*models.AppPackage, error) {
	appPkgRecord := &models.AppPackage{
		AppPkgId: appPkgId + tenantId + hostIp,
	}

	readErr := c.db.ReadData(appPkgRecord, util.AppPkgId)
	if readErr != nil {
		log.Error(util.AppPkgRecordDoesNotExit)
		return nil, readErr
	}
	return appPkgRecord, nil
}


