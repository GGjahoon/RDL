package data

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/GGjahon/bitcask-kv/fio"
)

var (
	ErrorEmptyKeyInFile = errors.New("the key in file is empty")
	ErrorDataDeleted    = errors.New("this key-value has been deleted")
)

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileID    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

func OpenDataFile(dirpath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirpath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	ioManager, err := fio.NewIoManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileID:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Get(offset int64) ([]byte, int64, *LogRecordHeader, error) {
	dataFileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, nil, err
	}
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > dataFileSize {
		headerBytes = dataFileSize - offset
	}

	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, nil, err
	}
	header := decodeLogRecordHeader(headerBuf)
	// 若读取到文件末尾 则返回eof错误
	if header == nil || (header.crc == 0 && header.keySize == 0 && header.valueSize == 0) {
		return nil, 0, nil, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	if keySize <= 0 {
		return nil, 0, nil, ErrorEmptyKeyInFile
	}
	var recordSize = int64(header.headerSize) + keySize + valueSize

	encLogRecord, err := df.readNBytes(recordSize, offset)
	if err != nil {
		return nil, 0, nil, err
	}

	return encLogRecord, recordSize, header, nil
}
func (df *DataFile) readNBytes(n int64, offset int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = df.IoManager.Read(buf, offset)
	return
}
