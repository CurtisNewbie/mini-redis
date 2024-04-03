package main

import "sync"

const (
	BufCap = 1024
)

var (
	bufferPool = sync.Pool{
		New: func() any {
			b := make([]byte, 1024)
			return &b
		},
	}
)

func GetBuf() *[]byte {
	bufp := bufferPool.Get().(*[]byte)
	*bufp = (*bufp)[:0]
	return bufp
}

func PutBuf(b *[]byte) {
	if cap(*b) <= BufCap {
		bufferPool.Put(b)
	}
}
