package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/GGjahon/bitcask-kv/data"
	goart "github.com/plar/go-adaptive-radix-tree"
)

// AdaPtiveRadixTree 自适应基数树索引  Index接口的art实现
type AdaPtiveRadixTree struct {
	Tree goart.Tree
	mu   *sync.RWMutex
}

func NewAdaPtiveRadixTree() Index {
	return &AdaPtiveRadixTree{
		Tree: goart.New(),
		mu:   new(sync.RWMutex),
	}
}

// Put the key into memory
func (art *AdaPtiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.mu.Lock()
	defer art.mu.Unlock()
	art.Tree.Insert(key, pos)
	return true
}

// Get the key's data store pos in disk
func (art *AdaPtiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.mu.RLock()
	defer art.mu.RUnlock()
	value, found := art.Tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Delete ,delete the key in memory
func (art *AdaPtiveRadixTree) Delete(key []byte) bool {
	art.mu.Lock()
	defer art.mu.Unlock()
	_, deleted := art.Tree.Delete(key)
	return deleted
}

// Iterator
func (art *AdaPtiveRadixTree) Iterator(reverse bool) Iterator {
	art.mu.RLock()
	defer art.mu.RUnlock()

	return newARTIterator(art.Tree, reverse)
}

// Size
func (art *AdaPtiveRadixTree) Size() int {
	art.mu.RLock()
	defer art.mu.RUnlock()
	size := art.Tree.Size()
	return size
}

// arttTeeIterator BTree索引迭代器实例
type artIterator struct {
	//当前遍历的下标位置
	currIndex int

	//reverse 表示是否为反向遍历
	reverse bool

	//values 储存从索引内加载的item数据
	values []*Item
}

func newARTIterator(tree goart.Tree, reverse bool) Iterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValue := func(node goart.Node) (cont bool) {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValue)
	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// ReWind() 回到迭代器起点
func (arti *artIterator) Rewind() {
	arti.currIndex = 0
}

// Seek() 根据传入的key查找到第一个大于（或小于）等于的目标key，从该key开始遍历
func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

// Next() 跳转到下一个key
func (arti *artIterator) Next() {
	arti.currIndex += 1
}

// Valid 验证是否有效，即是否遍历完成所有的key，用于退出遍历
func (arti *artIterator) Valid() bool {
	return arti.currIndex < len(arti.values)
}

// Key()当前遍历位置的key数据
func (arti *artIterator) Key() []byte {
	if !arti.Valid() {
		return nil
	}
	return arti.values[arti.currIndex].key
}

// Value 当前遍历位置的value数据
func (arti *artIterator) Value() *data.LogRecordPos {
	if !arti.Valid() {
		return nil
	}
	return arti.values[arti.currIndex].pos
}

// Close() 关闭迭代器，释放占用资源
func (arti *artIterator) Close() {
	arti.values = nil
}
