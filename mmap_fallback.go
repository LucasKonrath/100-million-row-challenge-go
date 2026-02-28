//go:build !linux && !darwin

package main

import (
	"io/ioutil"
	"os"
)

func mmap(file *os.File) ([]byte, error) {
	return ioutil.ReadAll(file)
}

func munmap(b []byte) error {
	return nil
}
