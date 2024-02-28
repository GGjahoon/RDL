package bitcaskkv

import (
	"bytes"

	"github.com/GGjahon/bitcask-kv/index"
)

type Iterator struct {
	Options   IterOptions
	indexIter index.Iterator
	db        *DB
}

func (db *DB) NewIterator(opts ...IterOption) *Iterator {
	itertor := &Iterator{
		Options: IterOptions{
			Prefix:  []byte(""),
			Reverse: false,
		},
		db: db,
	}

	for _, opt := range opts {
		opt(&itertor.Options)
	}

	itertor.indexIter = db.index.Iterator(itertor.Options.Reverse)
	return itertor
}

// ReWind() 回到迭代器起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek() 根据传入的key查找到第一个大于（或小于）等于的目标key，从该key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next() 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 验证是否有效，即是否遍历完成所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key()当前遍历位置的key数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的value数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	if logRecordPos == nil {
		return nil, ErrKeyIsNotFound
	}
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getLogRecordValue(logRecordPos)
}

// Close() 关闭迭代器，释放占用资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// skipToNext() 根据用户传入的prefix进行key的筛选
func (it *Iterator) skipToNext() {
	prefixLen := len(it.Options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Equal(it.Options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
