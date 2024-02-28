package bitcaskkv

import "github.com/GGjahon/bitcask-kv/index"

const (
	DefaultDirPath         = "./bitcask-kv-data"
	DefalutMaxDataFileSize = 128 * 1024 * 1024
	DefaultIndexType       = index.Btree
)

type Options struct {
	//数据库目录
	DirPath string

	//每个文件最大可写入大小
	MaxDataFileSize int64

	//是否每次写入均需要持久化
	SyncWrites bool

	//索引类型
	IndexType index.IndexTypes
}

type DBOption func(o *Options)

func WithDBDirPath(dirpath string) DBOption {
	return func(o *Options) {
		o.DirPath = dirpath
	}
}

func WithDBMaxDataFileSize(size int64) DBOption {
	return func(o *Options) {
		o.MaxDataFileSize = size
	}
}
func WithDBSync(is bool) DBOption {
	return func(o *Options) {
		o.SyncWrites = is
	}
}

func WithDBIndexType(indexType int8) DBOption {
	return func(o *Options) {
		o.IndexType = indexType
	}
}

func repaireDB(o *Options) {
	if len(o.DirPath) == 0 {
		o.DirPath = DefaultDirPath
	}
	if o.MaxDataFileSize <= 0 {
		o.MaxDataFileSize = DefalutMaxDataFileSize
	}
	if o.IndexType == 0 {
		o.IndexType = DefaultIndexType
	}
}

type IterOptions struct {
	Prefix []byte

	Reverse bool
}
type IterOption func(ito *IterOptions)

func WithIterPrefix(prefix []byte) IterOption {
	return func(ito *IterOptions) {
		ito.Prefix = prefix
	}
}
func WithIterReverse() IterOption {
	return func(ito *IterOptions) {
		ito.Reverse = true
	}
}

const DefaultMaxBatchNum = uint(100)

type WriteBatchOptions struct {
	MaxBatchNum uint

	SyncWrites bool
}
type WriteBatchOption func(o *WriteBatchOptions)

func WithMaxBatchNum(num uint) WriteBatchOption {
	return func(o *WriteBatchOptions) {
		o.MaxBatchNum = num
	}
}
