package storage

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMmap(t *testing.T) {
	dir := t.TempDir()
	filename := path.Join(dir, "test")

	f := MmapFile{}
	err := f.Init(filename)
	assert.Nil(t, err)
	defer f.Close()
}
