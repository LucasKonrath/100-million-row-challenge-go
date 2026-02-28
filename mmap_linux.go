//go:build linux

package main

import (
	"os"
	"syscall"
)

func mmap(file *os.File) ([]byte, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	return syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(b []byte) error {
	return syscall.Munmap(b)
}
