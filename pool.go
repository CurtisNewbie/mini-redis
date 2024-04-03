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

	valArrPool = sync.Pool{
		New: func() any {
			b := make([]Value, 5)
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

func GetArr() *[]Value {
	p := valArrPool.Get().(*[]Value)
	*p = (*p)[:0]
	return p
}

func PutArr(b *[]Value) {
	if b == nil {
		return
	}

	for i := range *b {
		it := (*b)[i]
		if it.typ == ArraysTyp {
			(*b)[i] = NilVal
			PutArr(it.arr)
		}
	}
	valArrPool.Put(b)
}
