//go:build !windows

package main

func runWindowsService(serviceName, configPath string) (bool, error) {
	return false, nil
}
