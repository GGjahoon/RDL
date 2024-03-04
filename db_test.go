package bitcaskkv

import (
	"fmt"
	"os"
	"testing"

	"github.com/GGjahon/bitcask-kv/index"
	"github.com/GGjahon/bitcask-kv/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenDBWithoutOptions(t *testing.T) {
	db, err := Open(WithDBDirPath("/tmp/udata"))
	require.NoError(t, err)
	require.Equal(t, db.Options.IndexType, index.Btree)
	require.Equal(t, db.Options.DirPath, "/tmp/udata")
	require.Equal(t, db.Options.MaxDataFileSize, int64(128*1024*1024))
	require.Equal(t, db.Options.SyncWrites, false)
}
func TestOpenDBWithOptions(t *testing.T) {
	db, err := Open(WithDBDirPath("../user1data"),
		WithDBIndexType(index.ARtree),
		WithDBMaxDataFileSize(150000),
		WithDBSync(true),
	)
	require.NoError(t, err)
	require.Equal(t, db.Options.IndexType, index.ARtree)
	require.Equal(t, db.Options.DirPath, "../user1data")
	require.Equal(t, db.Options.MaxDataFileSize, int64(150000))
	require.Equal(t, db.Options.SyncWrites, true)
}
func DeletedataFile() {
	for i := 0; i < 5; i++ {
		os.Remove(fmt.Sprintf("/home/jahoon/bitcask-kv/bitcask-kv-data/%09d.data", uint32(i)))
	}
}

func TestDelete(t *testing.T) {
	_, err := os.Stat("./userdata")
	fmt.Println(os.IsExist(err))
	t.Log(err)
}

func TestPutKV(t *testing.T) {
	testCases := []struct {
		name  string
		key   []byte
		value []byte
		Put   func(db *DB, key, val []byte)
		Get   func(db *DB, key []byte, expectVal []byte)
	}{
		{
			name:  "normal put",
			key:   utils.GetRandomKey(1),
			value: utils.GetRandomValue(24),
			Put: func(db *DB, key, val []byte) {
				err := db.Put(key, val)
				require.NoError(t, err)
			},
			Get: func(db *DB, key, expectVal []byte) {
				val, err := db.Get(key)
				require.NoError(t, err)
				require.Equal(t, val, expectVal)
			},
		},
		{
			name:  "put same key not same value",
			key:   utils.GetRandomKey(1),
			value: utils.GetRandomValue(24),
			Put: func(db *DB, key, val []byte) {
				err := db.Put(key, val)
				require.NoError(t, err)
			},
			Get: func(db *DB, key, expectVal []byte) {
				val, err := db.Get(key)
				require.NoError(t, err)
				require.Equal(t, val, expectVal)
			},
		},
		{
			name:  "key is empty",
			key:   []byte(""),
			value: utils.GetRandomValue(24),
			Put: func(db *DB, key, val []byte) {
				err := db.Put(key, val)
				require.ErrorIs(t, err, ErrKeyIsEmpty)
			},
			Get: func(db *DB, key, expectVal []byte) {

			},
		},
		{
			name:  "value is empty",
			key:   utils.GetRandomKey(1),
			value: []byte(""),
			Put: func(db *DB, key, val []byte) {
				err := db.Put(key, val)
				require.NoError(t, err)
			},
			Get: func(db *DB, key, expectVal []byte) {
				val, err := db.Get(key)
				require.NoError(t, err)
				require.Equal(t, val, expectVal)
			},
		},
		{
			name:  "restart db ",
			key:   utils.GetRandomKey(1),
			value: []byte(""),
			Put: func(db *DB, key, val []byte) {
				err := db.Put(key, val)
				require.NoError(t, err)
			},
			Get: func(db *DB, key, expectVal []byte) {
				val, err := db.Get(key)
				require.NoError(t, err)
				require.Equal(t, val, expectVal)
			},
		},
		{
			name:  "get invalid key",
			key:   []byte("some key unknown"),
			value: []byte(""),
			Put: func(db *DB, key, val []byte) {
			},
			Get: func(db *DB, key, expectVal []byte) {
				_, err := db.Get(key)
				require.ErrorIs(t, err, ErrKeyIsNotFound)

			},
		},
		// {
		// 	name:  "old files",
		// 	key:   utils.GetRandomKey(1),
		// 	value: utils.GetRandomValue(24),
		// 	Put: func(db *DB, key, val []byte) {
		// 		for i := 0; i < 2000000; i++ {
		// 			err := db.Put(utils.GetRandomKey(i), utils.GetRandomValue(128))
		// 			require.NoError(t, err)
		// 		}
		// 		//require.Equal(t, len(db.olderFiles), 1)
		// 		t.Log(db.activeFile.FileID)
		// 	},
		// 	Get: func(db *DB, key, expectVal []byte) {

		// 	},
		// },
	}
	db, err := Open()

	require.NoError(t, err)

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "put same key not same value" {
				tc.Put(db, tc.key, utils.GetRandomValue(32))
			}
			if tc.name == "restart db " {
				err := db.activeFile.Close()
				require.NoError(t, err)
				db, err = Open()
				require.NoError(t, err)
			}
			tc.Put(db, tc.key, tc.value)

			tc.Get(db, tc.key, tc.value)
		})
	}
	DeletedataFile()
}

func TestGetAndDelete(t *testing.T) {
	db, err := Open()
	require.NoError(t, err)

	//当前key被删除后再get
	//先写入
	key1 := utils.GetRandomKey(1)
	value1 := utils.GetRandomValue(24)
	err = db.Put(key1, value1)
	require.NoError(t, err)
	//get查看是否写入成功
	getValue1, err := db.Get(key1)
	require.NoError(t, err)
	require.Equal(t, value1, getValue1)
	//删除后再get
	db.Delete(key1)
	_, err = db.Get(key1)
	require.ErrorIs(t, err, ErrKeyIsNotFound)

	//插入一些数据
	key3 := utils.GetRandomKey(3)
	value3 := utils.GetRandomValue(32)
	key5 := utils.GetRandomKey(5)
	value5 := utils.GetRandomValue(32)
	db.Put(key3, value3)
	db.Put(key5, value5)

	//写入大量数据，从旧的数据文件上读取value

	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetRandomKey(i), utils.GetRandomValue(128))

		require.NoError(t, err)
	}
	//旧文件数量>1
	require.Greater(t, len(db.olderFiles), 0)
	getValue2, err := db.Get(utils.GetRandomKey(500))
	require.NoError(t, err)
	require.NotNil(t, getValue2)

	//重启数据库，即关闭actiefile后再openddb
	//关闭activeFile
	t.Log("close activeFile")
	err = db.activeFile.Close()
	require.NoError(t, err)
	//启动数据库实例
	t.Log("start to open db2")
	db2, err := Open()
	require.NoError(t, err)
	//获取此前写入的数据，验证
	getValue3, err := db2.Get(key3)
	require.NoError(t, err)
	require.Equal(t, getValue3, value3)

	getValue5, err := db2.Get(key5)
	require.NoError(t, err)

	require.Equal(t, getValue5, value5)
	DeletedataFile()
}

func TestListKeys(t *testing.T) {
	db, err := Open()
	defer DeletedataFile()
	assert.NotNil(t, db)
	assert.NoError(t, err)

	//空数据库尝试获取数据
	keys1 := db.ListKeys(false)
	assert.Equal(t, 0, len(keys1))

	keys := make([][]byte, 50)
	values := make([][]byte, 50)
	for i := 0; i < 50; i++ {
		keys[i] = utils.GetRandomKey(i)
		values[i] = utils.GetRandomValue(i)
		err := db.Put(keys[i], values[i])
		assert.NoError(t, err)
	}
	keys2 := db.ListKeys(false)
	assert.Equal(t, keys, keys2)
}

func TestFold(t *testing.T) {
	db, err := Open()
	defer DeletedataFile()
	assert.NotNil(t, db)
	assert.NoError(t, err)

	keys := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = utils.GetRandomKey(i)
		values[i] = utils.GetRandomValue(i)
		err := db.Put(keys[i], values[i])
		assert.NoError(t, err)
	}
	err = db.Fold(func(key, value []byte) bool {
		t.Log(string(key))
		t.Log(string(value))

		return true
	})
	assert.NoError(t, err)
}

// func TestOpenMergeDB(t *testing.T) {
// 	db, err := Open()
// 	assert.NoError(t, err)
// 	mergePath := db.getMergePath()

// 	mergeDB, err := Open(WithDBDirPath(mergePath))
// 	assert.NoError(t, err)
// 	assert.Equal(t, mergePath, mergeDB.Options.DirPath)
// }
