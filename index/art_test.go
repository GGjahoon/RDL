package index

import (
	"testing"

	"github.com/GGjahon/bitcask-kv/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArtGet(t *testing.T) {
	art := NewAdaPtiveRadixTree()
	Pres1 := art.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	require.True(t, Pres1)

	Gres1 := art.Get([]byte("abc"))
	require.Equal(t, Gres1.Fid, uint32(1))
	require.Equal(t, Gres1.Offset, int64(10))

	Pres2 := art.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    2,
		Offset: 12,
	})
	require.True(t, Pres2)
	Gres2 := art.Get([]byte("abc"))
	require.Equal(t, Gres2.Fid, uint32(2))
	require.Equal(t, Gres2.Offset, int64(12))

}
func TestARTree_Delete(t *testing.T) {
	art := NewAdaPtiveRadixTree()
	Pres1 := art.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	require.True(t, Pres1)

	Dres1 := art.Delete([]byte("abc"))
	require.True(t, Dres1)

	Gres1 := art.Get([]byte("abc"))
	t.Log(Gres1)

	Dres2 := art.Delete([]byte("abc"))
	require.False(t, Dres2)

}

func TestARTree_Iterator(t *testing.T) {

	var cur = []byte("abcdefg")

	art1 := NewAdaPtiveRadixTree()
	// Btree为空
	iter1 := art1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 插入数据
	ok1 := art1.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.Equal(t, ok1, true)
	iter2 := art1.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.Equal(t, iter2.Key(), []byte("a"))
	assert.Equal(t, iter2.Value().Fid, uint32(1))
	assert.Equal(t, iter2.Value().Offset, int64(10))

	iter2.Next()
	assert.Equal(t, 1, iter2.(*artIterator).currIndex)
	assert.False(t, iter1.Valid())

	//正向遍历
	art1.Put([]byte("b"), &data.LogRecordPos{Fid: 1, Offset: 10})
	art1.Put([]byte("c"), &data.LogRecordPos{Fid: 1, Offset: 10})
	art1.Put([]byte("d"), &data.LogRecordPos{Fid: 1, Offset: 10})
	art1.Put([]byte("e"), &data.LogRecordPos{Fid: 1, Offset: 10})
	art1.Put([]byte("f"), &data.LogRecordPos{Fid: 1, Offset: 10})
	art1.Put([]byte("g"), &data.LogRecordPos{Fid: 1, Offset: 10})
	size := art1.Size()
	require.Equal(t, 7, size)

	iter3 := art1.Iterator(false)
	for i := 0; i < len(cur); i++ {
		assert.Equal(t, cur[i], iter3.(*artIterator).values[i].key[0])
	}

	//反向遍历
	iter4 := art1.Iterator(true)
	for i := 0; i < len(cur); i++ {
		x := len(cur) - i - 1
		assert.Equal(t, cur[i], iter4.(*artIterator).values[x].key[0])
	}

	//rewind
	for i := 0; i < 4; i++ {
		iter4.Next()
	}
	assert.Equal(t, 4, iter4.(*artIterator).currIndex)

	iter4.Rewind()
	assert.Equal(t, 0, iter4.(*artIterator).currIndex)

	//seek
	//正序
	//正常情况
	iter3.Seek([]byte("d"))
	assert.Equal(t, []byte("d"), iter3.Key())
	iter3.Next()
	assert.Equal(t, []byte("e"), iter3.Key())
	//边界情况
	iter3.Seek([]byte("h"))
	assert.False(t, iter3.Valid())
	assert.Nil(t, iter3.Key())

	//倒序
	//正常情况
	iter4.Seek([]byte("e"))
	assert.Equal(t, []byte("e"), iter4.Key())
	iter4.Next()

	assert.Equal(t, []byte("d"), iter4.Key())
	//边界情况
	iter4.Seek([]byte("a"))
	iter4.Next()
	assert.False(t, iter4.Valid())
	assert.Nil(t, iter4.Key())

	iter1.Close()
	iter2.Close()
	iter3.Close()
	iter4.Close()
	//查看迭代器数组是否为nil
	assert.Nil(t, iter1.(*artIterator).values)
	assert.Nil(t, iter2.(*artIterator).values)
	assert.Nil(t, iter3.(*artIterator).values)
	assert.Nil(t, iter4.(*artIterator).values)
}
