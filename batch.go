package bitcaskkv

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/GGjahon/bitcask-kv/data"
	"github.com/GGjahon/bitcask-kv/index"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.RWMutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts ...WriteBatchOption) *WriteBatch {
	if db.IndexType == index.BPtree && !db.seqNoFExists && !db.isInitial {
		panic("cannot use write batch , seq no file not exists")
	}

	writeBatch := &WriteBatch{
		options: WriteBatchOptions{
			MaxBatchNum: DefaultMaxBatchNum,
			SyncWrites:  true,
		},
		mu:            new(sync.RWMutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
	for _, opt := range opts {
		opt(&writeBatch.options)
	}

	return writeBatch
}
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if uint(len(wb.pendingWrites)) == wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//判断当前key是否存在于索引中
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		//判断当前key是否存在于预写map中
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}
	//若存在于索引中且不存在于预写map中，将删除的logRecord存入map
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 获取当前事务序列号
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	//暂存插入数据的pos信息，待插入完成后再写入到内存
	positions := make(map[string]*data.LogRecordPos)
	for _, logRecord := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(logRecord.Key, seqNo),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}
		positions[string(logRecord.Key)] = logRecordPos
	}
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	_, err := wb.db.appendLogRecord(finishedRecord)
	if err != nil {
		return err
	}
	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	// 完成索引信息的插入
	for _, logRecord := range wb.pendingWrites {
		key := logRecord.Key
		pos := positions[string(key)]
		if logRecord.Type == data.LogRecordNormal {
			wb.db.index.Put(logRecord.Key, pos)
		}
		if logRecord.Type == data.LogRecordDeleted {
			wb.db.index.Delete(key)
		}
	}

	//清空预写数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
