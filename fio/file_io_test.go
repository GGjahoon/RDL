package fio

import (
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestNewFileIO(t *testing.T) {
	fio, err := NewFileIO(filepath.Join("./test", "a.data"))
	require.NoError(t, err)
	require.NotNil(t, fio)
}
func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIO(filepath.Join("./test", "a.data"))
	require.NoError(t, err)
	require.NotNil(t, fio)

	res1, err := fio.Write([]byte("jahoon"))
	require.Equal(t, res1, 6)
	require.NoError(t, err)

	res2, err := fio.Write([]byte("hello world"))
	require.Equal(t, res2, 11)
	require.NoError(t, err)
}

func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIO(filepath.Join("./test", "a.data"))
	require.NoError(t, err)
	require.NotNil(t, fio)
	b1 := make([]byte, 6)
	res1, err := fio.Read(b1, 0)
	require.NoError(t, err)
	t.Log(res1)
	t.Log(string(b1))
}
func TestFileIO_Sync(t *testing.T) {

}
