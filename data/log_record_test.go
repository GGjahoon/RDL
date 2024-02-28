package data

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogRecord(t *testing.T) {
	testCases := []struct {
		name      string
		logRecord *LogRecord
		setCRC    func(encodeLogRecord []byte) []byte
		check     func(t *testing.T, logRecord *LogRecord, afterLogRecord *LogRecord, err error)
	}{
		{
			name: "normal",
			logRecord: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("jahoon"),
				Type:  LogRecordNormal,
			},
			check: func(t *testing.T, logRecord *LogRecord, afterLogRecord *LogRecord, err error) {
				require.NoError(t, err)
				require.Equal(t, logRecord.Key, afterLogRecord.Key)
				require.Equal(t, logRecord.Value, afterLogRecord.Value)
				require.Equal(t, logRecord.Type, afterLogRecord.Type)
			},
		},
		{
			name: "value is empty",
			logRecord: &LogRecord{
				Key:  []byte("name"),
				Type: LogRecordNormal,
			},
			check: func(t *testing.T, logRecord *LogRecord, afterLogRecord *LogRecord, err error) {
				require.NoError(t, err)
				require.Equal(t, logRecord.Key, afterLogRecord.Key)
				require.Equal(t, []byte(""), afterLogRecord.Value)
				require.Equal(t, logRecord.Type, afterLogRecord.Type)
			},
		},
		{
			name: "crc is not equal",
			logRecord: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("jahoon"),
				Type:  LogRecordDeleted,
			},
			setCRC: func(encodeLogRecord []byte) []byte {
				encodeLogRecord[0] = byte(0)
				encodeLogRecord[1] = byte(0)
				return encodeLogRecord
			},
			check: func(t *testing.T, logRecord *LogRecord, afterLogRecord *LogRecord, err error) {
				require.ErrorIs(t, err, ErrorInvalidCRC)
			},
		},
		{
			name: "key-value has been deleted",
			logRecord: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("jahoon"),
				Type:  LogRecordDeleted,
			},
			check: func(t *testing.T, logRecord *LogRecord, afterLogRecord *LogRecord, err error) {
				require.NoError(t, err)
				require.Equal(t, logRecord.Key, afterLogRecord.Key)
				require.Equal(t, logRecord.Value, afterLogRecord.Value)
				require.Equal(t, logRecord.Type, afterLogRecord.Type)
			},
		},
		{
			name: "len buf < 5",
			logRecord: &LogRecord{
				Key:   []byte("name"),
				Value: []byte("jahoon"),
				Type:  LogRecordNormal,
			},
			setCRC: func(encodeLogRecord []byte) []byte {
				newLogRecord := encodeLogRecord[:4]
				return newLogRecord
			},
			check: func(t *testing.T, logRecord *LogRecord, afterLogRecord *LogRecord, err error) {
				require.ErrorIs(t, err, ErrorInvalidHeader)

			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			encLogRecord, _ := EnCodeLogRecord(tc.logRecord)
			if tc.name == "crc is not equal" || tc.name == "len buf < 5" {
				encLogRecord = tc.setCRC(encLogRecord)
			}

			header := decodeLogRecordHeader(encLogRecord)

			afterLogRecord, err := DecodeLogRecord(encLogRecord, header)

			tc.check(t, tc.logRecord, afterLogRecord, err)
		})

	}
}
