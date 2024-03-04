package index

import (
	"fmt"
	"path/filepath"

	"github.com/GGjahon/bitcask-kv/data"
	"go.etcd.io/bbolt"
)

const (
	btreeIndexFileName = "bptree-index"
)

var indexBucketName = []byte("bitcask-index")

// AdaPtiveRadixTree  B+树索引 Index接口的B+树实现
type BPlusTree struct {
	Tree *bbolt.DB
}

func NewBPlusTree(dirpath string, sync bool) Index {
	if dirpath == "bitcask-kv-data" {
		dirpath = "../bitcask-kv-data"
	}
	opts := bbolt.DefaultOptions
	opts.NoSync = !sync
	bpTree, err := bbolt.Open(filepath.Join(dirpath, btreeIndexFileName), 0644, opts)
	if err != nil {
		fmt.Println(err)
		panic("failed to open bpTree,")
	}
	//为bpTree初始化bucket
	if err := bpTree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		Tree: bpTree,
	}
}

// Put the key into memory
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.Tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncCodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

// Get the key's data store pos in disk
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.Tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecCodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

// Delete. delete the key in memory
func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.Tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); value != nil {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to deleted value")
	}
	return ok
}

// Iterator
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPTIterator(bpt.Tree, reverse)
}

// Size
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.Tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get bptree size")
	}
	return size
}

// bptTeeIterator B+Tree索引迭代器实例
type bptIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBPTIterator(tree *bbolt.DB, reverse bool) Iterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpti := &bptIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpti.Rewind()
	return bpti
}

// ReWind() 回到迭代器起点
func (bpti *bptIterator) Rewind() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Last()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.First()
	}
}

// Seek() 根据传入的key查找到第一个大于（或小于）等于的目标key，从该key开始遍历
func (bpti *bptIterator) Seek(key []byte) {
	bpti.currKey, bpti.currValue = bpti.cursor.Seek(key)
}

// Next() 跳转到下一个key
func (bpti *bptIterator) Next() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Prev()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.Next()
	}
}

// Valid 验证是否有效，即是否遍历完成所有的key，用于退出遍历
func (bpti *bptIterator) Valid() bool {
	return len(bpti.currKey) != 0
}

// Key()当前遍历位置的key数据
func (bpti *bptIterator) Key() []byte {
	return bpti.currKey
}

// Value 当前遍历位置的value数据
func (bpti *bptIterator) Value() *data.LogRecordPos {
	return data.DecCodeLogRecordPos(bpti.currValue)
}

// Close() 关闭迭代器，释放占用资源
func (bpti *bptIterator) Close() {
	bpti.tx.Rollback()
}
