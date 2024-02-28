package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAndCloseDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	t.Log(os.TempDir())
	require.NoError(t, err)
	require.NotNil(t, dataFile1)
	dataFile2, err := OpenDataFile(os.TempDir(), 1)
	require.NoError(t, err)
	require.NotNil(t, dataFile2)

	err = dataFile1.Close()
	require.NoError(t, err)

	err = dataFile2.Close()
	require.NoError(t, err)
}
func TestWriteData(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 2)
	require.NoError(t, err)
	require.NotNil(t, dataFile1)
	err = dataFile1.Write([]byte{176, 207, 127, 237, 0, 8, 12, 110, 97, 109, 101, 106, 97, 104, 111, 111, 110})

	require.NoError(t, err)

	err = dataFile1.Close()
	require.NoError(t, err)
}
func TestGetData(t *testing.T) {
	TestCases := []struct {
		name        string
		logRecord   *LogRecord
		checkResult func(encLogRecord []byte, getEncLogRecord []byte,
			getLogRecordHeader *LogRecordHeader, err error)
	}{
		{
			name: "normal",
			logRecord: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("jahoon"),
				Type:  LogRecordNormal,
			},
			checkResult: func(encLogRecord []byte, getEncLogRecord []byte,
				getLogRecordHeader *LogRecordHeader, err error) {
				require.NoError(t, err)
				require.Equal(t, encLogRecord, getEncLogRecord)
				require.Equal(t, getLogRecordHeader.recordType, LogRecordNormal)
			},
		},
		{
			name: "deleted",
			logRecord: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("bitcask-go"),
				Type:  LogRecordDeleted,
			},
			checkResult: func(encLogRecord []byte, getEncLogRecord []byte,
				getLogRecordHeader *LogRecordHeader, err error) {
				require.NoError(t, err)
				require.Equal(t, getLogRecordHeader.recordType, LogRecordDeleted)
			},
		},
	}
	var index int64
	for i := range TestCases {
		tc := TestCases[i]
		dataFile, err := OpenDataFile(os.TempDir(), 3)
		require.NoError(t, err)
		encLogRecord, size := EnCodeLogRecord(tc.logRecord)
		err = dataFile.Write(encLogRecord)
		require.NoError(t, err)

		getEncLogRecord, recordSize, getLogRecordHeader, err := dataFile.Get(int64(index))
		if tc.name == "normal" {
			require.Equal(t, size, recordSize)
		}

		tc.checkResult(encLogRecord, getEncLogRecord, getLogRecordHeader, err)

		index += size

		dataFile.WriteOff += size
		err = dataFile.Sync()
		require.NoError(t, err)

		// err = dataFile.Close()
		// require.NoError(t, err)
	}
}
