package bitcaskkv

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/GGjahon/bitcask-kv/data"
)

const (
	mergerDirName   = "-merge"
	mergeFinshedKey = "merge.finished"
)

// Merge 清除OldFiles中的无效数据，将数据文件整合， 生成Hint文件
func (db *DB) Merge() error {
	//若oldFiles文件数量为0,则直接返沪
	if len(db.olderFiles) == 0 {
		return nil
	}
	//上锁保证原子性操作
	db.mu.Lock()
	if db.isMerging {
		db.mu.Unlock()
		return ErrMErgeIsProgress
	}
	//修改标识位，标志当前有merge操作正在进行
	db.isMerging = true
	//持久化当前的activeFile，将当前activeFile添加进oldFileMap中，打开新的activeFile，记录其id
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileID] = db.activeFile
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	noMergeFileID := db.activeFile.FileID

	//从当前db实例中获取所有需要进行merge的数据文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	//拿到文件集合后解锁，并对mergeFiles进行排序
	db.mu.Unlock()
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	//打开新的mergeDB实例，进行merge操作，打开前需要查看记录数据的同一目录下是否存在merge目录，若存在，则删除
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		//说明此前存在过merge操作，将该目录删除，在之后创建mergeDB实例时自行创建
		if err := os.Remove(mergePath); err != nil {
			return err
		}
	}
	mergeDB, err := Open(WithDBDirPath(mergePath))
	if err != nil {
		return err
	}
	//生成hint文件，保存索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//遍历需要merge的文件，读取保存的数据
	for _, file := range mergeFiles {
		var offset int64 = 0
		for {
			encLogRecord, size, logRecordHeader, err := file.Get(offset)
			if err != nil {
				return err
			}
			logRecord, err := data.DecodeLogRecord(encLogRecord, logRecordHeader)
			if err != nil {
				return err
			}
			//获取真正的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			//判断当前logRecord是否是有效数据
			pos := db.index.Get(realKey)
			if pos != nil && pos.Fid == file.FileID && pos.Offset == offset {
				//代表当前数据的存储位置与内存索引中存储的值一致,为有效数据，将其写入到mergeFile中
				//写入前去掉之前key包含的事务id
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				logRecordPos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				//将当前数据的key和索引信息组合成logRecord形式，进行编码
				encPosRecord := data.EncPosLogRecordWithKeyAndPos(realKey, logRecordPos)
				if err := hintFile.Write(encPosRecord); err != nil {
					return err
				}
			}
			offset += size
		}

	}
	//遍历完成后，进行mergeDB的持久化，创建标志merge完成的FinishFile
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	finishFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeDoneRecord := &data.LogRecord{
		Key:   []byte(mergeFinshedKey),
		Value: []byte(strconv.Itoa(int(noMergeFileID))),
	}
	encMergeDoneRecord, _ := data.EnCodeLogRecord(mergeDoneRecord)
	if err := finishFile.Write(encMergeDoneRecord); err != nil {
		return err
	}
	return finishFile.Sync()
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.Options.DirPath))
	base := path.Base(db.Options.DirPath)
	return filepath.Join(dir, base+mergerDirName)
}

// loadMergeFiles 判断上次的merge是否完成，若完成，将merge过的数据文件进行删除，
// 将merge临时目录下的文件转移到db真正存储数据的目录进行替换
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	//判断当前是否存在有mergePath
	if _, err := os.Stat(mergePath); os.IsExist(err) {
		return nil
	}
	//在读取hint文件完成后，将merge目录删除
	defer func() {
		os.RemoveAll(mergePath)
	}()

	//读取mergePath下的所有文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	//若不存在mergeFinished标志文件，说明上次merge未完成，则直接返回即可
	if !mergeFinished {
		return nil
	}
	//获取此前未进行merge的activeFileID
	noMergeFileId, err := db.getNoMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//将merge过的数据文件进行删除操作
	var fileId uint32 = 0
	for ; fileId < noMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.Options.DirPath, fileId)
		//判断文件是否存在，若存在进行删除
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	//移动merge文件夹下所有文件到db.dirpath目录下
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		tarPath := filepath.Join(db.Options.DirPath, fileName)
		if err := os.Rename(srcPath, tarPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNoMergeFileId(dirPath string) (uint32, error) {
	mergeFF, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	//读取mergeFinishedFile中的数据
	encLogRecord, _, logRecordHeader, err := mergeFF.Get(0)
	if err != nil {
		return 0, err
	}
	mergeDoneLogRecord, err := data.DecodeLogRecord(encLogRecord, logRecordHeader)
	if err != nil {
		return 0, err
	}

	noMergeFileId, err := strconv.Atoi(string(mergeDoneLogRecord.Value))
	if err != nil {
		return 0, err
	}
	return uint32(noMergeFileId), nil
}

// loadIndexFromHintFile 从hint文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.DirPath, data.HintFileName)
	//先查看当前文件夹下是否存在hintFile,若不存在直接返回即可
	if _, err := os.Stat(hintFileName); os.IsExist(err) {
		return nil
	}
	//若存在，则打开文件，读取索引数据
	hinFile, err := data.OpenHintFile(db.DirPath)
	if err != nil {
		return err
	}
	var offset int64 = 0
	for {
		encPosLogRecord, size, posLogRecordHeader, err := hinFile.Get(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//解码record
		posLogRecord, err := data.DecodeLogRecord(encPosLogRecord, posLogRecordHeader)
		if err != nil {
			return err
		}
		pos := data.DecCodeLogRecordPos(posLogRecord.Value)
		//解码完成后，获取到key和key对应数据的pos,将key-pos放入内存索引即可
		db.index.Put(posLogRecord.Key, pos)

		//该条数据处理完成后，offset后移
		offset += size
	}
	return nil
}
