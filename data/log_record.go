package data

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type LogRecordType = byte

var (
	ErrorInvalidCRC    = errors.New("CRC is invalid ,the data file maybe corrupted")
	ErrorInvalidHeader = errors.New("logRecordHeader is nil")
)

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// LogRecordPos : the index of key in memory. LogRecordPos describe the position of data position in disk
type LogRecordPos struct {
	Fid    uint32 // the id of file in disk
	Offset int64  // the offset of data in the file
}

// LogRecord the data to write in disk
// 编码后的组成： crc校验(4字节) + recordType(1字节) + keySize(5变长字节) + valueSize(5变长字节)
const maxLogRecordHeaderSize = binary.MaxVarintLen32 + binary.MaxVarintLen32 + 5

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	headerSize uint32
	keySize    uint32
	valueSize  uint32
}

// EnCodeLogRecord 将LogRecord进行编码，返回byte数组和数组长度
func EnCodeLogRecord(LogRecord *LogRecord) ([]byte, int64) {
	// 构建header数组
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = LogRecord.Type
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(LogRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(LogRecord.Value)))

	encLogRecordSize := index + len(LogRecord.Key) + len(LogRecord.Value)
	//构造encLogecord数组
	encLogRecord := make([]byte, encLogRecordSize)
	//将header复制进encLogRecord
	copy(encLogRecord[:index], header[:index])
	copy(encLogRecord[index:], LogRecord.Key)
	copy(encLogRecord[(index+len(LogRecord.Key)):], LogRecord.Value)

	//校验
	crc := crc32.ChecksumIEEE(encLogRecord[4:])
	//将crc结果填入encLogRecord前段
	binary.LittleEndian.PutUint32(encLogRecord[:4], crc)
	return encLogRecord, int64(encLogRecordSize)

}

func DecodeLogRecord(buf []byte, header *LogRecordHeader) (*LogRecord, error) {
	if header == nil {
		return nil, ErrorInvalidHeader
	}
	logRecord := &LogRecord{
		Type: header.recordType,
	}
	index := int64(header.headerSize)

	keySize := int64(header.keySize)
	logRecord.Key = buf[index : index+keySize]

	index += keySize

	valueSize := int64(header.valueSize)
	logRecord.Value = buf[index : index+valueSize]

	headerBuf := buf[:int64(header.headerSize)]

	crc := getLogRecordCRC(logRecord, headerBuf)
	if crc != header.crc {
		return nil, ErrorInvalidCRC
	}
	return logRecord, nil
}
func decodeLogRecordHeader(buf []byte) *LogRecordHeader {
	if len(buf) < 5 {
		return nil
	}
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	header.headerSize = uint32(index)

	return header
}
func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	crc := crc32.ChecksumIEEE(header[4:])
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
}

// EncPosLogRecordWithKeyAndPos 使用索引信息构建posRecord，key作为实际的key，编码后的pos byte数组作为value进行编码
func EncPosLogRecordWithKeyAndPos(key []byte, pos *LogRecordPos) []byte {
	posLogRecord := &LogRecord{
		Key:   key,
		Value: EncCodeLogRecordPos(pos),
	}
	//编码posLogRecord
	encPosLogRecord, _ := EnCodeLogRecord(posLogRecord)
	return encPosLogRecord

}
func EncCodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}
func DecCodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fid, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fid),
		Offset: offset,
	}
}
