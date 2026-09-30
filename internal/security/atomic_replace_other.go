//go:build !windows

package security

import "os"

func replaceFileAtomic(source, target string) error {
	return os.Rename(source, target)
}

func syncParentDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
