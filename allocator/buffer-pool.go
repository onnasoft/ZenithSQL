package allocator

import (
	"fmt"
	"sync"
)

type BufferPool struct {
	pool sync.Pool
	size int
}

func NewBufferPool(capacity, size int) *BufferPool {
	return &BufferPool{
		size: capacity,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
	}
}

func (a *BufferPool) Allocate() ([]byte, error) {
	if a.size <= 0 {
		return nil, fmt.Errorf("buffer pool is empty")
	}

	buf := a.pool.Get()
	a.size--
	return buf.([]byte), nil
}

func (a *BufferPool) Release(buf interface{}) {
	if buf == nil {
		return
	}

	a.pool.Put(buf)
	a.size++
}
