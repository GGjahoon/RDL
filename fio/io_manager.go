package fio

const FilePerm = 0644

// IOManager , a interface for different IO,provides the method to operate the data in disk
type IOManager interface {
	// Write ,write the data into disk
	Write([]byte) (int, error)
	// Read ,read the target data in the disk
	Read([]byte, int64) (int, error)
	// Sync ,make the data in buffer into disk
	Sync() error
	// Close ,close the io
	Close() error
}
