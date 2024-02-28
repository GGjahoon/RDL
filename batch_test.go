package bitcaskkv

import (
	"testing"

	"github.com/GGjahon/bitcask-kv/utils"
	"github.com/stretchr/testify/assert"
)

func TestBatchWithOption(t *testing.T) {
	db, err := Open()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	wr := db.NewWriteBatch(WithMaxBatchNum(uint(10)))
	assert.NotNil(t, wr)
	assert.Equal(t, uint(10), wr.options.MaxBatchNum)

	for i := 0; i < 10; i++ {
		err := wr.Put(utils.GetRandomKey(i), utils.GetRandomValue(10))
		assert.NoError(t, err)
	}
	err = wr.Put(utils.GetRandomKey(11), utils.GetRandomValue(10))
	t.Log(len(wr.pendingWrites))
	assert.ErrorIs(t, err, ErrExceedMaxBatchNum)

}
func TestWriteBatch(t *testing.T) {
	db, err := Open()
	//defer DeletedataFile()
	assert.NoError(t, err)
	assert.NotNil(t, db)
	// 新建writeBatch
	wb := db.NewWriteBatch()
	assert.True(t, wb.options.SyncWrites)
	assert.Equal(t, uint(100), wb.options.MaxBatchNum)
	//writeBatch没有put数据，直接提交
	err = wb.Commit()
	assert.Nil(t, err)

	keys := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = utils.GetRandomKey(i)
		values[i] = utils.GetRandomValue(10)
		wb.Put(keys[i], values[i])
	}

	getValue, err := db.Get(utils.GetRandomKey(1))
	assert.ErrorIs(t, err, ErrKeyIsNotFound)
	assert.Nil(t, getValue)

	err = wb.Commit()
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		getValue, err = db.Get(keys[i])
		assert.NoError(t, err)
		assert.Equal(t, values[i], getValue)
	}
}

// // 为了模拟事务在commit时前段信息插入，但并未写入结束信息
// // 运行顺序：TestWriteBatch -> 注释掉batch.go的104～111行，为在最后不写入结束信息 ->TestBatchAndLoadIndex
// // 重新启动后应该在索引中仅有 000 - 010 的索引数据
// func TestWriteBatchWithoutFinshLogRecord(t *testing.T) {
// 	db, err := Open()
// 	//defer DeletedataFile()
// 	assert.NoError(t, err)
// 	assert.NotNil(t, db)
// 	// 新建writeBatch
// 	wb := db.NewWriteBatch()

// 	keys := make([][]byte, 10)
// 	values := make([][]byte, 10)
// 	for i := 10; i < 20; i++ {
// 		keys[i-10] = utils.GetRandomKey(i)
// 		values[i-10] = utils.GetRandomValue(10)
// 		wb.Put(keys[i-10], values[i-10])
// 	}
// 	wb.Commit()
// }

// // 运行前注释回来batch.go添加结束标志的逻辑代码
// func TestInsertOneRecord(t *testing.T) {
// 	db, err := Open()
// 	//defer DeletedataFile()
// 	assert.NoError(t, err)
// 	assert.NotNil(t, db)
// 	// 新建writeBatch
// 	wb := db.NewWriteBatch()
// 	wb.Put(utils.GetRandomKey(21), utils.GetRandomValue(10))
// 	wb.Commit()
// }

func TestBatchAndLoadIndex(t *testing.T) {
	db, err := Open()
	defer DeletedataFile()
	assert.NoError(t, err)
	assert.NotNil(t, db)
	for i := 0; i < 10; i++ {
		getvalue, err := db.Get(utils.GetRandomKey(1))
		assert.NoError(t, err)
		assert.NotNil(t, getvalue)
	}
	//确定索引内仅有10条数据
	assert.Equal(t, 10, db.index.Size())
	assert.Equal(t, uint64(1), db.seqNo)

	//尝试获取第11条，但不成功
	// getValue, err := db.Get(utils.GetRandomKey(11))
	// t.Log(err)
	// assert.ErrorIs(t, err, ErrKeyIsNotFound)
	// assert.Nil(t, getValue)

	// getValue1, err := db.Get(utils.GetRandomKey(21))
	// assert.NoError(t, err)
	// assert.NotNil(t, getValue1)
}
func TestBatchDelete(t *testing.T) {
	defer DeletedataFile()
	testCases := []struct {
		name           string
		PutValues      func(wr *WriteBatch)
		DeleteAndCheck func(wr *WriteBatch)
	}{
		{
			name: "delete key not commit",
			PutValues: func(wr *WriteBatch) {
				wr.Put(utils.GetRandomKey(1), utils.GetRandomValue(10))
			},
			DeleteAndCheck: func(wr *WriteBatch) {
				wr.Delete(utils.GetRandomKey(1))
				assert.Equal(t, 0, len(wr.pendingWrites))
			},
		},
		{
			name:      "delete empty key",
			PutValues: func(wr *WriteBatch) {},
			DeleteAndCheck: func(wr *WriteBatch) {
				err := wr.Delete([]byte(""))
				assert.ErrorIs(t, err, ErrKeyIsEmpty)
			},
		},
		{
			name: "delete the key in index",
			PutValues: func(wr *WriteBatch) {

			},
			DeleteAndCheck: func(wr *WriteBatch) {
				err := wr.Delete(utils.GetRandomKey(2))
				assert.NoError(t, err)
				err = wr.Commit()
				assert.NoError(t, err)
				getValue, err := wr.db.Get(utils.GetRandomKey(2))
				t.Log(err)
				assert.ErrorIs(t, err, ErrKeyIsNotFound)
				assert.Nil(t, getValue)
			},
		},
		{
			name:      "delete the key is not in index",
			PutValues: func(wr *WriteBatch) {},
			DeleteAndCheck: func(wr *WriteBatch) {
				err := wr.Delete(utils.GetRandomKey(3))
				assert.Nil(t, err)
			},
		},
	}

	//开启db实例 和 wr实例
	db, err := Open()
	assert.NoError(t, err)
	assert.NotNil(t, db)
	wr := db.NewWriteBatch()
	//提前插入一条数据以便之后删除
	err = db.Put(utils.GetRandomKey(2), utils.GetRandomValue(12))
	assert.NoError(t, err)
	getvalue, err := db.Get(utils.GetRandomKey(2))

	t.Log(getvalue)
	assert.NoError(t, err)
	assert.NotNil(t, getvalue)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.PutValues(wr)
			tc.DeleteAndCheck(wr)
		})
	}
}
