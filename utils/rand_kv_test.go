package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetRandomKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		key := GetRandomKey(i)
		t.Log(string(key))
		require.NotNil(t, key)
	}
}

func TestGetRandomValue(t *testing.T) {
	for i := 1; i < 10; i++ {
		value := GetRandomValue(i)
		t.Log(value)
		require.NotNil(t, value)
	}
}
