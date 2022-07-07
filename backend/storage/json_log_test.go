package storage

import (
	"testing"
)

func TestJsonLogHeader(t *testing.T) {
	t.Cleanup(delFile)
	testHeader(t, &JsonLog{})
}

func TestJsonLogLogEntry(t *testing.T) {
	t.Cleanup(delFile)
	testLogEntry(t, &JsonLog{})
}
