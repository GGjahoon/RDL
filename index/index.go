package index

import (
	"bytes"

	"github.com/GGjahon/bitcask-kv/data"
	"github.com/google/btree"
)

type IndexTypes = int8

const (
	Btree IndexTypes = iota + 1

	ARtree

	BPtree
)

// Index give all method for operating the key of data in memory.Every different could implement own method with this interface
type Index interface {
	// Put the key into memory
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get the key's data store pos in disk
	Get(key []byte) *data.LogRecordPos

	// Delete ,delete the key in memory
	Delete(key []byte) bool

	//Iterator
	Iterator(reverse bool) Iterator

	//Size
	Size() int
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
func NewIndex(typ IndexTypes, dirpath string, sync bool) Index {
	switch typ {
	case Btree:
		return NewBTree()
	case ARtree:
		return NewAdaPtiveRadixTree()
	case BPtree:
		return NewBPlusTree(dirpath, sync)
	default:
		panic("unsupported index type")
	}
}

// Iterator 抽象索引迭代器接口
type Iterator interface {
	// ReWind() 回到迭代器起点
	Rewind()

	//Seek() 根据传入的key查找到第一个大于（或小于）等于的目标key，从该key开始遍历
	Seek(key []byte)

	//Next() 跳转到下一个key
	Next()

	//Valid 验证是否有效，即是否遍历完成所有的key，用于退出遍历
	Valid() bool

	//Key()当前遍历位置的key数据
	Key() []byte

	//Value 当前遍历位置的value数据
	Value() *data.LogRecordPos

	//Close() 关闭迭代器，释放占用资源
	Close()
}
