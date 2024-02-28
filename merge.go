package bitcaskkv

import (
	"path"
	"path/filepath"
	"sort"

	"github.com/GGjahon/bitcask-kv/data"
)

const mergerDirName = "-merge"

// Merge 清除OldFiles中的无效数据，将数据文件整合， 生成Hint文件
func (db *DB) Merge() error {
	if len(db.olderFiles) == 0 {
		return nil
	}
	//上锁，保证更改db的merge标识位，更改activeFile 是基于锁的
	db.mu.Lock()
	if db.isMerging {
		//若merge正在进行中，直接返回
		db.mu.Unlock()
		return ErrMErgeIsProgress
	}
	db.isMerging = true
	//持久化当前activeFile
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileID] = db.activeFile
	//为db引擎打开新的activeFile
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 取出所有需要进行merge的oldFile
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()
	// 对mergeFIle进行排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	//mergePath := db.getMergePath()
	//判断此前是否存在merge目录，若存在则说明发生过merge，需要将该目录删掉
	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.Options.DirPath))
	base := path.Base(db.Options.DirPath)
	return filepath.Join(dir, base+mergerDirName)
}
