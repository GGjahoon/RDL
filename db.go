package bitcaskkv

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/GGjahon/bitcask-kv/data"
	"github.com/GGjahon/bitcask-kv/index"
)

// DB is a implement of bitcask for user
type DB struct {
	Options
	mu         *sync.RWMutex
	index      index.Index
	activeFile *data.DataFile
	fileIds    []int // 仅用于加载索引
	olderFiles map[uint32]*data.DataFile
	seqNo      uint64
	//标识是否正在进行merge 同一深刻下仅可有一个merge线程
	isMerging bool
}

func Open(opts ...DBOption) (*DB, error) {
	db := DB{
		Options:    Options{},
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
	}
	for _, opt := range opts {
		opt(&db.Options)
	}

	repaireDB(&db.Options)
	db.index = index.NewIndex(db.Options.IndexType)
	//判断用户输入的路径是否存在，若不存在，则帮用户创建该目录,若路径为db的默认路径，则无需创建
	if db.Options.DirPath != DefaultDirPath {
		if _, err := os.Stat(db.Options.DirPath); os.IsNotExist(err) {

			if err := os.Mkdir(db.Options.DirPath, os.ModePerm); err != nil {

				return nil, err
			}
		}
	}
	// 启动DB前，若目标目录中有老的.data文件，需要加载至db。
	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {

		return nil, err
	}

	// 循环读取datafile，将key读取至索引，储存在内存中
	if err := db.loadIndexFromDataFiles(); err != nil {

		return nil, err
	}
	return &db, nil
}

func (db *DB) loadDataFiles() error {
	entries, err := os.ReadDir(db.Options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	//遍历目录内所有文件，找到所有以 .data结尾的文件
	for _, entry := range entries {
		// 若文件后缀为 ".data" 则进行文件名分割
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 将文件id进行排序
	sort.Ints(fileIds)
	// 为了之后有序加载index，将排序后的fileIds添加到DB结构体中
	db.fileIds = fileIds
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	fileNums := len(db.fileIds)
	if fileNums == 0 {
		return nil
	}
	updateIndex := func(key []byte, typ data.LogRecordType, logRecordPos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, logRecordPos)
		}
		if !ok {
			panic("failed to update index at start up")
		}
	}
	//若读取到通过事务提交的数据，则暂存在该map中
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo
	//遍历所有文件，处理文件中的记录，将key加载至index中
	for i, fid := range db.fileIds {

		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			encLogRecord, size, logRecordHeader, err := dataFile.Get(offset)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					return err
				}
			}
			logRecord, err := data.DecodeLogRecord(encLogRecord, logRecordHeader)
			if err != nil {
				return err
			}
			//根据解码后的logRecord构建内存中将要存储的目标key-fileid,offset,存储至db的index中
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			//解码从文件中读出数据的真正key
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//若是通过事务提交的，将数据暂存在map数组中，等待读取到事务结束标志，再进行索引更新
				if logRecord.Type == data.LogRecordTxnFinished {
					//更新暂存数据的索引
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					//更新完成后删除map内的数据
					delete(transactionRecords, seqNo)
				} else {
					//当前数据通过事务进行提交，还未读取到相应的结束标志，先暂存。
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size
		}

		// 若读取到最后一个文件，即activeFile，需要将该activeFile的offset写入db结构体内
		if i == fileNums-1 {
			db.activeFile.WriteOff = offset
		}
	}

	db.seqNo = currentSeqNo

	return nil
}

// Put 逻辑，将 key - value写入到数据文件，并将key - offset 写入内存索引
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构建即将要写入的 LogRecord   普通put ，将key编码为 uint64(0) + key
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)

	if err != nil {
		return err
	}
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	//判断db当前的活跃文件是否存在，若不存在，需要初始化活跃文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	//此后将执行实际LogRecord写入文件
	//将LogRecord结构体进行编码
	encLogRecord, logRecordSize := data.EnCodeLogRecord(logRecord)
	//判断当前活跃文件是否有足够空间写入当前logRecord
	if db.activeFile.WriteOff+logRecordSize > db.Options.MaxDataFileSize {
		//对当前活跃文件进行持久化
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//旧活跃文件添加至map
		db.olderFiles[db.activeFile.FileID] = db.activeFile

		//打开新的活跃文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encLogRecord); err != nil {
		return nil, err
	}

	if db.Options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOff,
	}
	return pos, nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if key == nil {
		return nil, ErrKeyIsEmpty
	}

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyIsNotFound
	}

	return db.getLogRecordValue(logRecordPos)
}
func (db *DB) ListKeys(reverse bool) [][]byte {

	iterator := db.index.Iterator(reverse)

	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx += 1
	}
	return keys
}
func (db *DB) getLogRecordValue(logRecordPos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	// 判断该数据的存储文件是否为当前活跃文件
	if logRecordPos.Fid == db.activeFile.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// 判断文件是否存在
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//读取数据
	encLogRecord, _, logRecordHeader, err := dataFile.Get(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//将读出的数据进行解码
	logRecord, err := data.DecodeLogRecord(encLogRecord, logRecordHeader)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyIsNotFound
	}
	return logRecord.Value, nil
}

type FoldFunc func(key []byte, value []byte) bool

func (db *DB) Fold(ff FoldFunc) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getLogRecordValue(iterator.Value())
		if err != nil {
			return err
		}
		if !ff(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	pos := db.index.Get(key)
	if pos == nil {
		return nil
	}
	deleteLogRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecordWithLock(deleteLogRecord)
	if err != nil {
		return nil
	}
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// setActiveFile 设置db当前的活跃文件
func (db *DB) setActiveFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileID + 1
	}
	dataFile, err := data.OpenDataFile(db.Options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func (db *DB) Close() error {
	if db.activeFile == nil && len(db.olderFiles) == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	//关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	return nil
}
