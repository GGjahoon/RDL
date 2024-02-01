package data

// LogRecordPos : the index of key in memory. LogRecordPos describe the position of data position in disk
type LogRecordPos struct {
	Fid    uint32 // the id of file in disk
	Offset int64  // the offset of data in the file
}
