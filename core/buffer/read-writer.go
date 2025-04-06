package buffer

import (
	"sync"
)

type MReadWriter struct {
	buf    *Buffer
	offset int
	mu     sync.Mutex
}

func NewReadWriter(buf *Buffer) ReadWriter {
	return &MReadWriter{buf: buf}
}

func (rw *MReadWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	err := rw.buf.Write(rw.offset, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (rw *MReadWriter) Read(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	data, err := rw.buf.Read(rw.offset, len(p))
	if err != nil {
		return 0, err
	}
	copy(p, data)
	return len(data), nil
}

func (rw *MReadWriter) ReadAt(p []byte, offset int) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	data, err := rw.buf.Read(rw.offset+offset, len(p))
	if err != nil {
		return 0, err
	}
	copy(p, data)
	return len(data), nil
}

func (rw *MReadWriter) Offset() int {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.offset
}

func (rw *MReadWriter) Seek(offset int) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.offset = offset
}
