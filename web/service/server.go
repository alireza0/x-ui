package service

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/alireza0/x-ui/config"
	"github.com/alireza0/x-ui/database"
	"github.com/alireza0/x-ui/logger"
	"github.com/alireza0/x-ui/util/common"
	"github.com/alireza0/x-ui/util/sys"
	"github.com/alireza0/x-ui/xray"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
)

type ProcessState string

const (
	Running ProcessState = "Running"
	Stop    ProcessState = "Stop"
	Error   ProcessState = "Error"
)

type Status struct {
	T        time.Time `json:"-"`
	Cpu      float64   `json:"cpu"`
	CpuCount int       `json:"cpuCount"`
	Mem      struct {
		Current uint64 `json:"current"`
		Total   uint64 `json:"total"`
	} `json:"mem"`
	Swap struct {
		Current uint64 `json:"current"`
		Total   uint64 `json:"total"`
	} `json:"swap"`
	Disk struct {
		Current uint64 `json:"current"`
		Total   uint64 `json:"total"`
	} `json:"disk"`
	Xray struct {
		State    ProcessState `json:"state"`
		ErrorMsg string       `json:"errorMsg"`
		Version  string       `json:"version"`
	} `json:"xray"`
	Uptime   uint64    `json:"uptime"`
	Loads    []float64 `json:"loads"`
	TcpCount int       `json:"tcpCount"`
	UdpCount int       `json:"udpCount"`
	NetIO    struct {
		Up   uint64 `json:"up"`
		Down uint64 `json:"down"`
	} `json:"netIO"`
	NetTraffic struct {
		Sent uint64 `json:"sent"`
		Recv uint64 `json:"recv"`
	} `json:"netTraffic"`
	AppStats struct {
		Threads uint32 `json:"threads"`
		Mem     uint64 `json:"mem"`
		Uptime  uint64 `json:"uptime"`
	} `json:"appStats"`
	HostInfo struct {
		HostName string `json:"hostname"`
		Ipv4     string `json:"ipv4"`
		Ipv6     string `json:"ipv6"`
	} `json:"hostInfo"`
}

type Release struct {
	TagName string `json:"tag_name"`
	Id      uint   `json:"id"`
}

type ServerService struct {
	xrayService        XrayService
	inboundService     InboundService
	outboundService    OutboundService
	routingRuleService RoutingRuleService
}

func (s *ServerService) GetStatus(lastStatus *Status) *Status {
	now := time.Now()
	status := &Status{
		T: now,
	}

	percents, err := cpu.Percent(0, false)
	if err != nil {
		logger.Warning("get cpu percent failed:", err)
	} else {
		status.Cpu = percents[0]
	}

	upTime, err := host.Uptime()
	if err != nil {
		logger.Warning("get uptime failed:", err)
	} else {
		status.Uptime = upTime
	}

	memInfo, err := mem.VirtualMemory()
	if err != nil {
		logger.Warning("get virtual memory failed:", err)
	} else {
		status.Mem.Current = memInfo.Used
		status.Mem.Total = memInfo.Total
	}

	swapInfo, err := mem.SwapMemory()
	if err != nil {
		logger.Warning("get swap memory failed:", err)
	} else {
		status.Swap.Current = swapInfo.Used
		status.Swap.Total = swapInfo.Total
	}

	distInfo, err := disk.Usage("/")
	if err != nil {
		logger.Warning("get dist usage failed:", err)
	} else {
		status.Disk.Current = distInfo.Used
		status.Disk.Total = distInfo.Total
	}

	avgState, err := load.Avg()
	if err != nil {
		logger.Warning("get load avg failed:", err)
	} else {
		status.Loads = []float64{avgState.Load1, avgState.Load5, avgState.Load15}
	}

	ioStats, err := net.IOCounters(false)
	if err != nil {
		logger.Warning("get io counters failed:", err)
	} else if len(ioStats) > 0 {
		ioStat := ioStats[0]
		status.NetTraffic.Sent = ioStat.BytesSent
		status.NetTraffic.Recv = ioStat.BytesRecv

		if lastStatus != nil {
			duration := now.Sub(lastStatus.T)
			seconds := float64(duration) / float64(time.Second)
			up := uint64(float64(status.NetTraffic.Sent-lastStatus.NetTraffic.Sent) / seconds)
			down := uint64(float64(status.NetTraffic.Recv-lastStatus.NetTraffic.Recv) / seconds)
			status.NetIO.Up = up
			status.NetIO.Down = down
		}
	} else {
		logger.Warning("can not find io counters")
	}

	status.TcpCount, err = sys.GetTCPCount()
	if err != nil {
		logger.Warning("get tcp connections failed:", err)
	}

	status.UdpCount, err = sys.GetUDPCount()
	if err != nil {
		logger.Warning("get udp connections failed:", err)
	}

	if s.xrayService.IsXrayRunning() {
		status.Xray.State = Running
		status.Xray.ErrorMsg = ""
	} else {
		err := s.xrayService.GetXrayErr()
		if err != nil {
			status.Xray.State = Error
		} else {
			status.Xray.State = Stop
		}
		status.Xray.ErrorMsg = s.xrayService.GetXrayResult()
	}
	status.Xray.Version = s.xrayService.GetXrayVersion()

	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)

	status.AppStats.Mem = rtm.Sys
	status.AppStats.Threads = uint32(runtime.NumGoroutine())
	status.CpuCount = runtime.NumCPU()
	if p != nil && p.IsRunning() {
		status.AppStats.Uptime = p.GetUptime()
	} else {
		status.AppStats.Uptime = 0
	}

	status.HostInfo.HostName, _ = os.Hostname()

	// get ip address
	netInterfaces, _ := net.Interfaces()
	for i := 0; i < len(netInterfaces); i++ {
		if len(netInterfaces[i].Flags) > 2 && netInterfaces[i].Flags[0] == "up" && netInterfaces[i].Flags[1] != "loopback" {
			addrs := netInterfaces[i].Addrs

			for _, address := range addrs {
				if strings.Contains(address.Addr, ".") {
					status.HostInfo.Ipv4 += address.Addr + " "
				} else if address.Addr[0:6] != "fe80::" {
					status.HostInfo.Ipv6 += address.Addr + " "
				}
			}
		}
	}

	return status
}

func (s *ServerService) GetXrayVersions() ([]string, error) {
	url := "https://api.github.com/repos/XTLS/Xray-core/releases"
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	buffer := bytes.NewBuffer(make([]byte, 8192))
	buffer.Reset()
	_, err = buffer.ReadFrom(resp.Body)
	if err != nil {
		return nil, err
	}

	releases := make([]Release, 0)
	err = json.Unmarshal(buffer.Bytes(), &releases)
	if err != nil {
		return nil, err
	}
	var versions []string
	for _, release := range releases {
		versions = append(versions, release.TagName)
		// Limit: v26.10.10
		if release.Id <= 408835648 {
			break
		}
	}
	return versions, nil
}

func (s *ServerService) StopXrayService() (string error) {
	err := s.xrayService.StopXray()
	if err != nil {
		logger.Error("stop xray failed:", err)
		return err
	}

	return nil
}

func (s *ServerService) RestartXrayService() (string error) {
	s.xrayService.StopXray()
	defer func() {
		err := s.xrayService.RestartXray(true)
		if err != nil {
			logger.Error("start xray failed:", err)
		}
	}()

	return nil
}

func (s *ServerService) downloadXRay(version string) (string, error) {
	osName := runtime.GOOS
	arch := runtime.GOARCH

	switch osName {
	case "darwin":
		osName = "macos"
	case "windows":
		osName = "windows"
	}

	switch arch {
	case "amd64":
		arch = "64"
	case "arm64":
		arch = "arm64-v8a"
	case "armv7":
		arch = "arm32-v7a"
	case "armv6":
		arch = "arm32-v6"
	case "armv5":
		arch = "arm32-v5"
	case "386":
		arch = "32"
	case "s390x":
		arch = "s390x"
	}

	fileName := fmt.Sprintf("Xray-%s-%s.zip", osName, arch)
	url := fmt.Sprintf("https://github.com/XTLS/Xray-core/releases/download/%s/%s", version, fileName)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	os.Remove(fileName)
	file, err := os.Create(fileName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", err
	}

	return fileName, nil
}

func (s *ServerService) UpdateXray(version string) error {
	// 1. Stop xray before doing anything
	if err := s.StopXrayService(); err != nil {
		logger.Warning("failed to stop xray before update:", err)
	}

	// 2. Download the zip
	zipFileName, err := s.downloadXRay(version)
	if err != nil {
		return err
	}
	defer os.Remove(zipFileName)

	zipFile, err := os.Open(zipFileName)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	stat, err := zipFile.Stat()
	if err != nil {
		return err
	}
	reader, err := zip.NewReader(zipFile, stat.Size())
	if err != nil {
		return err
	}

	// 3. Helper to extract files
	copyZipFile := func(zipName string, fileName string) error {
		zipFile, err := reader.Open(zipName)
		if err != nil {
			return err
		}
		defer zipFile.Close()
		os.MkdirAll(filepath.Dir(fileName), 0755)
		os.Remove(fileName)
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fs.ModePerm)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(file, zipFile)
		return err
	}

	// 4. Extract correct binary
	if runtime.GOOS == "windows" {
		targetBinary := filepath.Join("bin", "xray-windows-amd64.exe")
		err = copyZipFile("xray.exe", targetBinary)
	} else {
		err = copyZipFile("xray", xray.GetBinaryPath())
	}
	if err != nil {
		return err
	}

	// 5. Restart xray
	if err := s.xrayService.RestartXray(true); err != nil {
		logger.Error("start xray failed:", err)
		return err
	}

	return nil
}

func (s *ServerService) GetLogs(count string, level string, syslog string) []string {
	c, _ := strconv.Atoi(count)
	var lines []string

	if syslog == "true" {
		// Check if running on Windows - journalctl is not available
		if runtime.GOOS == "windows" {
			return []string{"Syslog is not supported on Windows. Please use application logs instead by unchecking the 'Syslog' option."}
		}

		// Validate and sanitize count parameter
		countInt, err := strconv.Atoi(count)
		if err != nil || countInt < 1 || countInt > 10000 {
			return []string{"Invalid count parameter - must be a number between 1 and 10000"}
		}

		// Validate level parameter - only allow valid syslog levels
		validLevels := map[string]bool{
			"0": true, "emerg": true,
			"1": true, "alert": true,
			"2": true, "crit": true,
			"3": true, "err": true,
			"4": true, "warning": true,
			"5": true, "notice": true,
			"6": true, "info": true,
			"7": true, "debug": true,
		}
		if !validLevels[level] {
			return []string{"Invalid level parameter - must be a valid syslog level"}
		}

		// Use hardcoded command with validated parameters
		cmd := exec.Command("journalctl", "-u", "x-ui", "--no-pager", "-n", strconv.Itoa(countInt), "-p", level)
		var out bytes.Buffer
		cmd.Stdout = &out
		err = cmd.Run()
		if err != nil {
			return []string{"Failed to run journalctl command! Make sure systemd is available and x-ui service is registered."}
		}
		lines = strings.Split(out.String(), "\n")
	} else {
		lines = logger.GetLogs(c, level)
	}

	return lines
}

func (s *ServerService) GetConfigJson() (interface{}, error) {
	config, err := s.xrayService.GetXrayConfig()
	if err != nil {
		return nil, err
	}
	contents, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, err
	}

	var jsonData interface{}
	err = json.Unmarshal(contents, &jsonData)
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func (s *ServerService) GetDb() ([]byte, error) {
	// Update by manually trigger a checkpoint operation
	err := database.Checkpoint()
	if err != nil {
		return nil, err
	}
	// Open the file for reading
	file, err := os.Open(config.GetDBPath())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the file contents
	fileContents, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return fileContents, nil
}

func (s *ServerService) ImportDB(file multipart.File) error {
	// Check if the file is a SQLite database
	isValidDb, err := database.IsSQLiteDB(file)
	if err != nil {
		return common.NewErrorf("Error checking db file format: %v", err)
	}
	if !isValidDb {
		return common.NewError("Invalid db file format")
	}

	// Reset the file reader to the beginning
	_, err = file.Seek(0, 0)
	if err != nil {
		return common.NewErrorf("Error resetting file reader: %v", err)
	}

	// Save the file as temporary file
	tempPath := fmt.Sprintf("%s.temp", config.GetDBPath())
	// Remove the existing fallback file (if any) before creating one
	_, err = os.Stat(tempPath)
	if err == nil {
		errRemove := os.Remove(tempPath)
		if errRemove != nil {
			return common.NewErrorf("Error removing existing temporary db file: %v", errRemove)
		}
	}
	// Create the temporary file
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return common.NewErrorf("Error creating temporary db file: %v", err)
	}
	defer tempFile.Close()

	// Remove temp file before returning
	defer os.Remove(tempPath)

	// Save uploaded file to temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		return common.NewErrorf("Error saving db: %v", err)
	}

	// Close temp file before opening via sqlite
	if err = tempFile.Close(); err != nil {
		return common.NewErrorf("Error closing temporary db file: %v", err)
	}
	tempFile = nil

	// Validate integrity (no migrations / side effects)
	if err = database.ValidateSQLiteDB(tempPath); err != nil {
		return common.NewErrorf("Invalid or corrupt db file: %v", err)
	}

	// Stop Xray (ignore error but log)
	if errStop := s.StopXrayService(); errStop != nil {
		logger.Warningf("Failed to stop Xray before DB import: %v", errStop)
	}

	// Close existing DB to release file locks (especially on Windows)
	if errClose := database.CloseDB(); errClose != nil {
		logger.Warningf("Failed to close existing DB before replacement: %v", errClose)
	}

	// Backup the current database for fallback
	fallbackPath := fmt.Sprintf("%s.backup", config.GetDBPath())
	// Remove the existing fallback file (if any)
	_, err = os.Stat(fallbackPath)
	if err == nil {
		errRemove := os.Remove(fallbackPath)
		if errRemove != nil {
			return common.NewErrorf("Error removing existing fallback db file: %v", errRemove)
		}
	}
	// Move the current database to the fallback location
	err = os.Rename(config.GetDBPath(), fallbackPath)
	if err != nil {
		return common.NewErrorf("Error backing up temporary db file: %v", err)
	}

	// Remove the temporary file before returning
	defer os.Remove(fallbackPath)

	// Move temp to DB path
	err = os.Rename(tempPath, config.GetDBPath())
	if err != nil {
		errRename := os.Rename(fallbackPath, config.GetDBPath())
		if errRename != nil {
			return common.NewErrorf("Error moving db file and restoring fallback: %v", errRename)
		}
		return common.NewErrorf("Error moving db file: %v", err)
	}

	// Migrate DB
	err = database.InitDB(config.GetDBPath())
	if err != nil {
		errRename := os.Rename(fallbackPath, config.GetDBPath())
		if errRename != nil {
			return common.NewErrorf("Error migrating db and restoring fallback: %v", errRename)
		}
		return common.NewErrorf("Error migrating db: %v", err)
	}
	// Start Xray
	err = s.RestartXrayService()
	if err != nil {
		return common.NewErrorf("Imported DB but Failed to start Xray: %v", err)
	}

	return nil
}

func (s *ServerService) GetNewX25519Cert() (interface{}, error) {
	// Run the command
	cmd := exec.Command(xray.GetBinaryPath(), "x25519")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(out.String(), "\n")

	privateKeyLine := strings.Split(lines[0], ":")
	publicKeyLine := strings.Split(lines[1], ":")

	privateKey := strings.TrimSpace(privateKeyLine[1])
	publicKey := strings.TrimSpace(publicKeyLine[1])

	keyPair := map[string]interface{}{
		"privateKey": privateKey,
		"publicKey":  publicKey,
	}

	return keyPair, nil
}

func (s *ServerService) GetNewmldsa65() (any, error) {
	// Run the command
	cmd := exec.Command(xray.GetBinaryPath(), "mldsa65")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(out.String(), "\n")

	SeedLine := strings.Split(lines[0], ":")
	VerifyLine := strings.Split(lines[1], ":")

	seed := strings.TrimSpace(SeedLine[1])
	verify := strings.TrimSpace(VerifyLine[1])

	keyPair := map[string]any{
		"seed":   seed,
		"verify": verify,
	}

	return keyPair, nil
}

func (s *ServerService) GetNewEchCert(sni string) (interface{}, error) {
	// Run the command
	cmd := exec.Command(xray.GetBinaryPath(), "tls", "ech", "--serverName", sni)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(out.String(), "\n")
	if len(lines) < 4 {
		return nil, common.NewError("invalid ech cert")
	}

	configList := lines[1]
	serverKeys := lines[3]

	return map[string]interface{}{
		"echServerKeys": serverKeys,
		"echConfigList": configList,
	}, nil
}

func (s *ServerService) GetNewVlessEnc() (any, error) {
	cmd := exec.Command(xray.GetBinaryPath(), "vlessenc")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	lines := strings.Split(out.String(), "\n")
	var auths []map[string]string
	var current map[string]string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Authentication:") {
			if current != nil {
				auths = append(auths, current)
			}
			current = map[string]string{
				"label": strings.TrimSpace(strings.TrimPrefix(line, "Authentication:")),
			}
		} else if strings.HasPrefix(line, `"decryption"`) || strings.HasPrefix(line, `"encryption"`) {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 && current != nil {
				key := strings.Trim(parts[0], `" `)
				val := strings.Trim(parts[1], `" `)
				current[key] = val
			}
		}
	}

	if current != nil {
		auths = append(auths, current)
	}

	return map[string]any{
		"auths": auths,
	}, nil
}
