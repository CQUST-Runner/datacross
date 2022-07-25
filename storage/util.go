package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

func GenUUID() (string, error) {
	data := [16]byte{}
	n, err := rand.Read(data[:])
	if err != nil {
		return "", err
	}
	if n != len(data) {
		return "", fmt.Errorf("generate random numbers failed")
	}

	sb := strings.Builder{}
	sb.Grow(36)
	sb.WriteString(hex.EncodeToString(data[:4]))
	sb.WriteByte('-')
	sb.WriteString(hex.EncodeToString(data[4:6]))
	sb.WriteByte('-')
	sb.WriteString(hex.EncodeToString(data[6:8]))
	sb.WriteByte('-')
	sb.WriteString(hex.EncodeToString(data[8:10]))
	sb.WriteByte('-')
	sb.WriteString(hex.EncodeToString(data[10:]))
	return sb.String(), nil
}

func IsFile(filename string) bool {
	stat, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return stat.Mode().IsRegular()
}

func IsDir(filename string) bool {
	stat, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return stat.Mode().IsDir()
}
