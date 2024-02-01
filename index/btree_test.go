package index

import (
	"github.com/GGjahon/bitcask-kv/data"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	Pres1 := bt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	require.True(t, Pres1)

	Gres1 := bt.Get([]byte("abc"))
	require.Equal(t, Gres1.Fid, uint32(1))
	require.Equal(t, Gres1.Offset, int64(10))

	Pres2 := bt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    2,
		Offset: 12,
	})
	require.True(t, Pres2)
	Gres2 := bt.Get([]byte("abc"))
	require.Equal(t, Gres2.Fid, uint32(2))
	require.Equal(t, Gres2.Offset, int64(12))

}
func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	Pres1 := bt.Put([]byte("abc"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	require.True(t, Pres1)

	Dres1 := bt.Delete([]byte("abc"))
	require.True(t, Dres1)

	Gres1 := bt.Get([]byte("abc"))
	t.Log(Gres1)

	Dres2 := bt.Delete([]byte("abc"))
	require.False(t, Dres2)

}
