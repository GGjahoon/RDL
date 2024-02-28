package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyz")
)

func GetRandomKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-kv-key%09d", i))
}
func GetRandomValue(n int) []byte {
	val := make([]byte, n)
	for i := range val {
		val[i] = letters[randStr.Intn(len(letters))]
	}
	return val
}
