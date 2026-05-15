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

package config

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/ghodss/yaml"
	log "github.com/sirupsen/logrus"
	"k8splugin/util"
)

// Ak sk and appInsId info
type AppAuthConfigBuilder struct {
	AppInsId string
	Ak       string
	Sk       string
	// [新增] 2026-01-19 网络平面功能：接收前端传递的参数（如 networkPlane）
	// 用于在部署阶段将抽象的网络平面选择转换为具体的 K8s Multus 注解
	Parameters map[string]string
}

// Constructor to Application configuration
// [修改] 2026-01-19 网络平面功能：添加 parameters 参数
// 原始签名: func NewBuildAppAuthConfig(appInsId string, ak string, sk string)
func NewBuildAppAuthConfig(appInsId string, ak string, sk string, parameters map[string]string) (appAuthCfg AppAuthConfigBuilder) {
	appAuthCfg.AppInsId = appInsId
	appAuthCfg.Ak = ak
	appAuthCfg.Sk = sk
	// [新增] 接收并保存前端传递的参数（包括 networkPlane）
	appAuthCfg.Parameters = parameters
	return
}

// extract the tar.gz file
func (_ *AppAuthConfigBuilder) extractTarFile(gzipStream io.Reader) (string, error) {

	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		log.Error("failed to read the file")
		return "", err
	}


	dirName, err := processTarFile(uncompressedStream)
	if err != nil {
		log.Error("failed to process the tar file")
		return "", err
	}

	defer uncompressedStream.Close()
	return dirName, nil
}

// Process tar file
func processTarFile(uncompressedStream *gzip.Reader) (string, error) {
	var dirName []string
	var count = 0
	var totalWrote int64
	fileCount := 0

	tarReader := tar.NewReader(uncompressedStream)
	for true {
		header, err := tarReader.Next()
		if err == io.EOF || header == nil {
			break
		}

		if header.Typeflag == tar.TypeDir {
			_ = os.MkdirAll(header.Name, 0755)
		} else if header.Typeflag == tar.TypeReg {
			dir, _ := filepath.Split(header.Name)
			if count == 0 {
				dirName = strings.Split(dir, "/")
				count += 1
			}
			tw, err := handleRegularFile(dir, header, tarReader, totalWrote, fileCount)
			if err != nil {
				return "", err
			}
			totalWrote = tw
		}
		fileCount++
	}
	return dirName[0], nil
}

// Handle regular file
func handleRegularFile(dir string, header *tar.Header, tarReader *tar.Reader,
	totalWrote int64, fileCount int) (int64, error) {
	if fileCount > util.TooManyFile {
		log.Error("too many files contains in tar file")
		return totalWrote, errors.New("too many files contains in tar file")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Error("failed to create the directory")
		return totalWrote, err
	}
	outFile, err := os.Create(header.Name)
	if err != nil {
		log.Error("failed to create the file")
		return totalWrote, err
	}
	if header.Size > util.SingleFileTooBig || totalWrote > util.TooBig {
		log.Error("size of the file is too big")
		return totalWrote, err
	}
	defer outFile.Close()
	wt, err := io.Copy(outFile, tarReader)
	if err != nil {
		log.Error("failed to copy the file")
		return totalWrote, err
	}
	totalWrote += wt
	return totalWrote, nil
}

// update values yaml file
func (appAuthCfg *AppAuthConfigBuilder) addAppAuthCfgInValuesFile(configPath string) (string, error) {
	values, err := ioutil.ReadFile(configPath + "/values.yaml")
	if err != nil {
		log.Error("Failed to read values yaml file")
		return "", err
	}

	jsondata, err := yaml.YAMLToJSON(values)
	if err != nil {
		log.Error("Failed to convert yaml to json")
		return "", err
	}

	var appAuthConfig map[string]interface{}
	err = json.Unmarshal(jsondata, &appAuthConfig)
	if err != nil {
		log.Error("Failed to unmarshal appAuthConfig")
		return "", err
	}
	appConfig := appAuthConfig["appconfig"]
	appConfig1 := appConfig.(map[string]interface{})

	// ========== [原有逻辑] 处理 AKSK 配置 ==========
	akskInfo := appConfig1["aksk"]
	akskConfig := akskInfo.(map[string]interface{})
	akskConfig["appInsId"] = appAuthCfg.AppInsId
	akskConfig["accesskey"] = appAuthCfg.Ak
	akskConfig["secretkey"] = appAuthCfg.Sk
	akskConfig["secretname"] = util.RandomSecretName(10)

	// ========== [新增] 2026-01-19 网络平面功能：处理网络平面参数 ==========
	// [NetworkPlane DEBUG] 节点5: appconfigbuilder 处理参数，注入 podAnnotations
	log.Infof("[NetworkPlane] [5/5] addAppAuthCfgInValuesFile, appInsId=%s, parameters count=%d",
		appAuthCfg.AppInsId, len(appAuthCfg.Parameters))
	for k, v := range appAuthCfg.Parameters {
		log.Infof("[NetworkPlane] [5/5]   param key=%s  value=%s", k, v)
	}
	if appAuthCfg.Parameters != nil {
		networkPlane, hasNetworkPlane := appAuthCfg.Parameters["networkPlane"]
		log.Infof("[NetworkPlane] [5/5] networkPlane key exists=%v, value='%s'", hasNetworkPlane, networkPlane)
		if hasNetworkPlane && networkPlane != "" && networkPlane != "default" {
			// 构造 Multus 注解值
			multusAnnotation := buildMultusAnnotation(networkPlane)
			log.Infof("[NetworkPlane] [5/5] buildMultusAnnotation('%s') => '%s'", networkPlane, multusAnnotation)
			if multusAnnotation != "" {
				log.Infof("[NetworkPlane] [5/5] Injecting annotation k8s.v1.cni.cncf.io/networks=%s", multusAnnotation)
				// 注入到 podAnnotations 节点
				if appAuthConfig["podAnnotations"] == nil {
					appAuthConfig["podAnnotations"] = make(map[string]interface{})
					log.Info("[NetworkPlane] [5/5] podAnnotations key not exist, created new map")
				} else {
					log.Info("[NetworkPlane] [5/5] podAnnotations key already exists, appending")
				}
				podAnnotations := appAuthConfig["podAnnotations"].(map[string]interface{})
				podAnnotations["k8s.v1.cni.cncf.io/networks"] = multusAnnotation
				log.Infof("[NetworkPlane] [5/5] SUCCESS: injected Multus annotation into podAnnotations")
			} else {
				log.Warnf("[NetworkPlane] [5/5] SKIP: buildMultusAnnotation returned empty for networkPlane='%s'", networkPlane)
				log.Warn("[NetworkPlane] [5/5]   --> check buildMultusAnnotation switch cases, value may use wrong separator (- vs _)")
			}
		} else {
			log.Infof("[NetworkPlane] [5/5] SKIP: using default cluster network (hasKey=%v, value='%s')", hasNetworkPlane, networkPlane)
		}
	} else {
		log.Warn("[NetworkPlane] [5/5] SKIP: appAuthCfg.Parameters is nil, no parameters received")
	}

	injectResourceRequests(appAuthConfig, appAuthCfg.Parameters)
	injectHostPathMount(appAuthConfig, appAuthCfg.Parameters)
	if err := injectResourceRequestsIntoTemplates(configPath, appAuthCfg.Parameters); err != nil {
		log.Warnf("[ResourceRequest] inject into templates skipped: %v", err)
	}

	// 序列化回 YAML
	appAuthInfo, err := yaml.Marshal(&appAuthConfig)
	if err != nil {
		log.Error("Failed to marshal appAuthConfig")
		return "", err
	}

	err = ioutil.WriteFile(configPath + "/values.yaml", appAuthInfo, 0644)
	if err != nil {
		log.Error("Failed to update values yaml file")
		return "", err
	}
	nameSpace := fmt.Sprintf("%v", appConfig1["appnamespace"])
	return nameSpace, nil
}

// create a tar file
func (_ *AppAuthConfigBuilder) createTarFile(source, target string) error {
	filename := filepath.Base(source)

	target = filepath.Join(target, fmt.Sprintf("%s.tar.gz", filename))
	tarfile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer tarfile.Close()

	// set up the gzip writer
	gw := gzip.NewWriter(tarfile)
	defer gw.Close()

	tarball := tar.NewWriter(gw)
	defer tarball.Close()

	info, err := os.Stat(source)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(source)
	}

	return filepath.Walk(source,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				return err
			}

			if baseDir != "" {
				header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, source))
			}


			if err := tarball.WriteHeader(header); err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(tarball, file)
			return err
		})
}

// add ak, sk and appInsId values in values yaml file
func (appAuthCfg *AppAuthConfigBuilder) AddValues(tarFile *os.File) (string, string, error) {
	dirName, err := appAuthCfg.extractTarFile(tarFile)
	if err != nil {
		log.Error("Unable to extract tar file")
		return "", "", err
	}

	namespace, err := appAuthCfg.addAppAuthCfgInValuesFile(dirName)
	if err != nil {
		return "", "", err
	}

	err = appAuthCfg.createTarFile(dirName, "./")
	if err != nil {
		log.Error("Failed to create a tar.gz file")
		return "", "", err
	}

	return dirName, namespace, nil
}

// ========== [新增] 2026-01-19 网络平面功能：辅助函数 ==========

// buildMultusAnnotation 将抽象的网络平面参数转换为具体的 K8s Multus CNI NetworkAttachmentDefinition 名称
// 功能说明：
//   - 前端传递抽象的网络平面选择（如 "physical_direct"、"n6_net_1"、"n6_net_2"）
//   - 此函数将其映射到 K8s 集群中已预先创建的 NetworkAttachmentDefinition 资源名称
//   - 这些 NAD 资源由集群管理员预先配置，定义了具体的网络接口类型（如 SR-IOV、Macvlan 等）
//
// 参数映射说明（实际测试环境配置）：
//   - "physical_direct" 或 "n6_net_1" -> "n6-net-1": N6 网络平面1（物理直通网络）
//   - "n6_net_2"                      -> "n6-net-2": N6 网络平面2（备用网络）
//   - "default" 或空值                 -> "": 不注入 Multus 注解，使用 K8s 默认集群网络
//
// 注意事项：
//   1. 返回的 NAD 名称必须与 K8s 集群中实际创建的 NetworkAttachmentDefinition 资源名称一致
//   2. 如果 NAD 不存在，Pod 创建时会失败，需要集群管理员预先配置
//   3. 可根据实际部署环境扩展更多网络平面类型的映射
func buildMultusAnnotation(networkPlane string) string {
	switch networkPlane {
	case "n6-net-1":
		// 物理直通平面 / N6 网络平面1：映射到实际测试环境的 n6-net-1
		// K8s 集群中已创建名为 "n6-net-1" 的 NetworkAttachmentDefinition
		// 用于 5G 核心网 UPF N6 接口或物理直通场景
		return "n6-net-1"
	case "n6-net-2":
		// N6 网络平面2：映射到实际测试环境的 n6-net-2
		// K8s 集群中已创建名为 "n6-net-2" 的 NetworkAttachmentDefinition
		// 用于备用网络或多接口场景
		return "n6-net-2"
	case "default":
		// 明确指定使用默认网络，不注入 Multus 注解
		return "default"
	default:
		// 未知的网络平面类型，返回空字符串，不注入注解
		// 此时应用将使用 K8s 默认的集群网络（通常是 Flannel/Calico 等 CNI）
		log.Warnf("[NetworkPlane] Unknown network plane type: %s, will use default cluster network", networkPlane)
		return ""
	}
}

func injectResourceRequests(values map[string]interface{}, parameters map[string]string) {
	if parameters == nil {
		log.Info("[ResourceRequest] SKIP: parameters is nil")
		return
	}

	cpuRequest := strings.TrimSpace(parameters["cpuRequest"])
	memoryRequest := normalizeMemoryRequest(parameters["memoryRequest"])
	if cpuRequest == "" && memoryRequest == "" {
		log.Info("[ResourceRequest] SKIP: no cpuRequest or memoryRequest received")
		return
	}
	if cpuRequest == "" || memoryRequest == "" {
		log.Warnf("[ResourceRequest] SKIP: cpuRequest and memoryRequest must be provided together, cpu='%s', memory='%s'",
			cpuRequest, parameters["memoryRequest"])
		return
	}
	if !isPositiveCPURequest(cpuRequest) {
		log.Warnf("[ResourceRequest] SKIP: invalid cpuRequest='%s'", cpuRequest)
		return
	}

	resources := getOrCreateMap(values, "resources")
	requests := getOrCreateMap(resources, "requests")
	requests["cpu"] = cpuRequest
	requests["memory"] = memoryRequest
	log.Infof("[ResourceRequest] SUCCESS: injected resources.requests cpu=%s memory=%s", cpuRequest, memoryRequest)
}

func injectResourceRequestsIntoTemplates(chartDir string, parameters map[string]string) error {
	if parameters == nil {
		return errors.New("parameters is nil")
	}

	cpuRequest := strings.TrimSpace(parameters["cpuRequest"])
	memoryRequest := normalizeMemoryRequest(parameters["memoryRequest"])
	if cpuRequest == "" || memoryRequest == "" {
		return errors.New("cpuRequest/memoryRequest empty")
	}
	if !isPositiveCPURequest(cpuRequest) {
		return errors.New("cpuRequest invalid")
	}
	if memoryRequest == "" {
		return errors.New("memoryRequest invalid")
	}

	templatesDir := filepath.Join(chartDir, "templates")
	if _, err := os.Stat(templatesDir); err != nil {
		return err
	}

	// Match a single-line YAML map: resources: {}
	// Capture indentation so we can generate correct nested indentation under containers.
	re := regexp.MustCompile(`(?m)^(\s*)resources:\s*\{\s*\}\s*$`)

	return filepath.Walk(templatesDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") {
			return nil
		}

		raw, readErr := ioutil.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		text := string(raw)
		if !re.MatchString(text) {
			return nil
		}

		updated := re.ReplaceAllStringFunc(text, func(m string) string {
			sub := re.FindStringSubmatch(m)
			indent := ""
			if len(sub) >= 2 {
				indent = sub[1]
			}
			indent2 := indent + "  "
			indent4 := indent + "    "
			return fmt.Sprintf("%sresources:\n%srequests:\n%scpu: \"%s\"\n%smemory: \"%s\"",
				indent, indent2, indent4, cpuRequest, indent4, memoryRequest)
		})

		if updated == text {
			return nil
		}
		if writeErr := ioutil.WriteFile(path, []byte(updated), info.Mode()); writeErr != nil {
			return writeErr
		}
		log.Infof("[ResourceRequest] updated template resources placeholder: %s", path)
		return nil
	})
}

func getOrCreateMap(parent map[string]interface{}, key string) map[string]interface{} {
	if existing, ok := parent[key].(map[string]interface{}); ok {
		return existing
	}
	next := make(map[string]interface{})
	parent[key] = next
	return next
}

func isPositiveCPURequest(cpuRequest string) bool {
	value, err := strconv.ParseFloat(cpuRequest, 64)
	return err == nil && value > 0
}

func normalizeMemoryRequest(memoryRequest string) string {
	memoryRequest = strings.TrimSpace(memoryRequest)
	if memoryRequest == "" {
		return ""
	}
	if strings.HasSuffix(memoryRequest, "Gi") {
		value := strings.TrimSuffix(memoryRequest, "Gi")
		if isPositiveInteger(value) {
			return memoryRequest
		}
		return ""
	}
	if strings.HasSuffix(memoryRequest, "Mi") {
		value := strings.TrimSuffix(memoryRequest, "Mi")
		if isPositiveInteger(value) {
			return memoryRequest
		}
		return ""
	}
	if isPositiveInteger(memoryRequest) {
		return memoryRequest + "Mi"
	}
	return ""
}

func isPositiveInteger(value string) bool {
	parsed, err := strconv.Atoi(value)
	return err == nil && parsed > 0
}

// Kubernetes volume names: DNS subdomain label, max 63 chars.
var k8sVolumeNameRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func isValidK8sVolumeName(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	return k8sVolumeNameRegexp.MatchString(name)
}

func injectHostPathMount(values map[string]interface{}, parameters map[string]string) {
	if parameters == nil {
		log.Info("[HostPathMount] SKIP: parameters is nil")
		return
	}
	mountType := strings.TrimSpace(parameters["hostPathMountType"])
	if mountType != "hostPath" {
		log.Infof("[HostPathMount] SKIP: mount type not hostPath (got %q)", mountType)
		return
	}
	hostPathVal := strings.TrimSpace(parameters["hostPath"])
	mountPathVal := strings.TrimSpace(parameters["containerMountPath"])
	volumeName := strings.TrimSpace(parameters["volumeName"])
	if hostPathVal == "" || mountPathVal == "" || volumeName == "" {
		log.Warn("[HostPathMount] SKIP: hostPath, containerMountPath or volumeName empty")
		return
	}
	if !strings.HasPrefix(hostPathVal, "/") || !strings.HasPrefix(mountPathVal, "/") {
		log.Warn("[HostPathMount] SKIP: paths must be absolute")
		return
	}
	if !isValidK8sVolumeName(volumeName) {
		log.Warnf("[HostPathMount] SKIP: invalid volumeName %q", volumeName)
		return
	}
	hm := getOrCreateMap(values, "hostPathExtraMount")
	hm["enabled"] = true
	hm["name"] = volumeName
	hm["hostPath"] = hostPathVal
	hm["type"] = "DirectoryOrCreate"
	hm["mountPath"] = mountPathVal
	log.Infof("[HostPathMount] SUCCESS: enabled name=%s hostPath=%s mountPath=%s", volumeName, hostPathVal, mountPathVal)
}
