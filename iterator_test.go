package bitcaskkv

import (
	"fmt"
	"testing"

	"github.com/GGjahon/bitcask-kv/utils"
	"github.com/stretchr/testify/assert"
)

const (
	prefix = "bitcask-kv-key"
)

func TestNoPrefixIterator(t *testing.T) {
	db, err := Open()
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer DeletedataFile()
	keys := make([][]byte, 50)
	values := make([][]byte, 50)
	for i := 0; i < 50; i++ {
		keys[i] = utils.GetRandomKey(i)
		values[i] = utils.GetRandomValue(i)
		err := db.Put(keys[i], values[i])
		assert.NoError(t, err)
	}

	iter := db.NewIterator()
	assert.NotNil(t, iter)
	var j = 0

	for iter.Rewind(); iter.Valid(); iter.Next() {
		getKey := iter.Key()
		getValue, err := iter.Value()
		assert.NoError(t, err)
		assert.Equal(t, getKey, keys[j])
		assert.Equal(t, getValue, values[j])
		j++
	}

	//seek测试
	j = 10

	iter.Seek(keys[j])

	for iter.Seek(keys[j]); iter.Valid(); iter.Next() {
		getKey := iter.Key()
		getValue, err := iter.Value()
		assert.NoError(t, err)
		assert.Equal(t, getKey, keys[j])
		assert.Equal(t, getValue, values[j])
		j++
	}
	//跳转到最后一条，再找寻下一条，应该返回无法找到key错误
	iter.Seek(keys[49])
	iter.Next()
	assert.Nil(t, iter.Key())
	val, err := iter.Value()
	assert.ErrorIs(t, err, ErrKeyIsNotFound)
	assert.Nil(t, val)

	iter.Close()
}
func TestPrefixIterator(t *testing.T) {
	db, err := Open()
	assert.NoError(t, err)

	assert.NotNil(t, db)
	defer DeletedataFile()
	keys := make([][]byte, 10)
	values := make([][]byte, 10)

	keys2 := make([][]byte, 10)
	values2 := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = utils.GetRandomKey(i)
		values[i] = utils.GetRandomValue(i)

		tmpKey := fmt.Sprintf("normal key %09d", i)
		tmpValue := fmt.Sprintf("normal value %09d", i)
		keys2[i] = []byte(tmpKey)
		values2[i] = []byte(tmpValue)
	}

	for i := 0; i < 20; i++ {
		if i%2 == 0 {
			err := db.Put(keys[i/2], values[i/2])
			assert.NoError(t, err)
		} else {
			err := db.Put(keys2[i/2], values2[i/2])
			assert.NoError(t, err)
		}
	}
	count := 0
	iter2 := db.NewIterator(WithIterPrefix([]byte(prefix)))

	for iter2.Rewind(); iter2.Valid(); iter2.Next() {

		assert.Equal(t, keys[count], iter2.Key())
		getValue, err := iter2.Value()
		assert.NoError(t, err)
		assert.Equal(t, values[count], getValue)

		count++

	}

}

// func TestRemove(t *testing.T) {
// 	os.RemoveAll("bitcask-kv-data-merge")
// }
