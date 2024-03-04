package index

import (
	"os"
	"testing"

	"github.com/GGjahon/bitcask-kv/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const path = "bitcask-kv-data"

func DeleteBPTreeFile() {
	if _, err := os.Stat("../bitcask-kv-data/bptree-index"); err == nil {
		os.Remove("../bitcask-kv-data/bptree-index")
	}
}
func TestBPTree_Get(t *testing.T) {
	bpt := NewBPlusTree(path, true)
	Pres1 := bpt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	require.True(t, Pres1)

	Gres1 := bpt.Get([]byte("abc"))
	require.Equal(t, Gres1.Fid, uint32(1))
	require.Equal(t, Gres1.Offset, int64(10))

	Pres2 := bpt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    2,
		Offset: 12,
	})
	require.True(t, Pres2)
	Gres2 := bpt.Get([]byte("abc"))
	require.Equal(t, Gres2.Fid, uint32(2))
	require.Equal(t, Gres2.Offset, int64(12))
	DeleteBPTreeFile()
}

func TestBPTree_Delete(t *testing.T) {
	bpt := NewBPlusTree(path, true)
	Pres1 := bpt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	require.True(t, Pres1)

	Dres1 := bpt.Delete([]byte("abc"))
	require.True(t, Dres1)

	Gres1 := bpt.Get([]byte("abc"))
	t.Log(Gres1)

	Dres2 := bpt.Delete([]byte("abc"))
	require.False(t, Dres2)

	DeleteBPTreeFile()

}

func TestBPTree_Iterator(t *testing.T) {

	var cur = []byte("abcdefg")

	bpt1 := NewBPlusTree(path, true)

	// Btree为空
	iter1 := bpt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())
	iter1.Close()

	//插入数据
	ok1 := bpt1.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.Equal(t, ok1, true)
	iter2 := bpt1.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.Equal(t, iter2.Key(), []byte("a"))
	assert.Equal(t, iter2.Value().Fid, uint32(1))
	assert.Equal(t, iter2.Value().Offset, int64(10))

	assert.Equal(t, []byte("a"), iter2.(*bptIterator).currKey)
	iter2.Next()
	assert.False(t, iter2.Valid())
	iter2.Close()

	// //正向遍历
	bpt1.Put([]byte("b"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bpt1.Put([]byte("c"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bpt1.Put([]byte("d"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bpt1.Put([]byte("e"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bpt1.Put([]byte("f"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bpt1.Put([]byte("g"), &data.LogRecordPos{Fid: 1, Offset: 10})
	//测试btree.Size()
	size := bpt1.Size()
	assert.Equal(t, 7, size)
	iter3 := bpt1.Iterator(false)
	iter3.Rewind()
	for i := 0; i < len(cur); i++ {
		assert.Equal(t, cur[i], iter3.Key()[0])
		iter3.Next()
	}

	iter3.Close()
	//反向遍历
	iter4 := bpt1.Iterator(true)
	for i := 0; i < len(cur); i++ {
		x := len(cur) - i - 1
		assert.Equal(t, cur[x], iter4.Key()[0])
		iter4.Next()
	}
	iter4.Close()

	iter5 := bpt1.Iterator(true)

	//rewind
	for i := 0; i < 4; i++ {
		iter5.Next()

	}

	assert.Equal(t, []byte("c"), iter5.Key())

	iter5.Rewind()
	assert.Equal(t, []byte("g"), iter5.Key())

	iter5.Seek([]byte("h"))
	t.Log(string(iter5.Key()))
	assert.Nil(t, iter5.Key())
	// //seek
	// //正序
	// //正常情况
	// iter3.Seek([]byte("d"))
	// assert.Equal(t, []byte("d"), iter3.Key())
	// iter3.Next()
	// assert.Equal(t, []byte("e"), iter3.Key())
	// //边界情况
	// iter3.Seek([]byte("h"))
	// assert.False(t, iter3.Valid())
	// assert.Nil(t, iter3.Key())

	// //倒序
	// //正常情况
	// iter4.Seek([]byte("e"))
	// assert.Equal(t, []byte("e"), iter4.Key())
	// iter4.Next()

	// assert.Equal(t, []byte("d"), iter4.Key())
	// //边界情况
	// iter4.Seek([]byte("a"))
	// iter4.Next()
	// assert.False(t, iter4.Valid())
	// assert.Nil(t, iter4.Key())

	DeleteBPTreeFile()
}
