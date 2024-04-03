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
	return bufferPool.Get().([]byte)
}

func PutBuf(b []byte) {
	if cap(b) <= BufCap {
		b = b[:0]
		bufferPool.Put(b)
	}
}
