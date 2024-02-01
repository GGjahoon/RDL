package fio

import (
	"os"
)

// FileIO an implement of IOManager , include all method of IOManager
type FileIO struct {
	fd *os.File
}

func NewFileIO(filename string) (IOManager, error) {
	fd, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		FilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{
		fd: fd,
	}, nil
}

// Write ,write the data into disk
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Read ,read the target data in the disk
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Sync ,make the data in buffer into disk
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close ,close the io
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
