package main

import "sync"

const (
	BufCap = 1024
)

var (
	bufferPool = sync.Pool{
		New: func() any {
			return make([]byte, 1024)
		},
	}
)

func GetBuf() []byte {
	b := bufferPool.Get().([]byte)
	b = b[:0]
	return b
}

func PutBuf(b []byte) {
	if cap(b) <= BufCap {
		bufferPool.Put(b)
	}
}
