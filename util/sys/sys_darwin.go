//go:build darwin
// +build darwin

package sys

import (
	"syscall"

	"github.com/shirou/gopsutil/v4/net"
)

var SIGUSR1 = syscall.SIGUSR1

func GetTCPCount() (int, error) {
	stats, err := net.Connections("tcp")
	if err != nil {
		return 0, err
	}
	return len(stats), nil
}

func GetUDPCount() (int, error) {
	stats, err := net.Connections("udp")
	if err != nil {
		return 0, err
	}
	return len(stats), nil
}
