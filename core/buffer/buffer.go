package buffer

import (
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const pageSize = 1024 * 1024 * 256 // 256MB

type Buffer struct {
	data         []byte
	length       int
	file         *os.File
	mu           sync.Mutex
	syncPeriod   time.Duration
	writeCh      chan struct{}
	stopCh       chan struct{}
	writeMode    bool
	writeModeMux sync.RWMutex
}

func NewBufferWithSize(path string, size int64) (*Buffer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	if size == 0 {
		size = pageSize
	}

	stats, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := stats.Size()

	if fileSize != size {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to truncate file: %w", err)
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to sync file: %w", err)
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	b := &Buffer{
		data:       data,
		length:     int(size),
		file:       f,
		syncPeriod: time.Second,
		writeCh:    make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}

	go b.syncLoop()
	return b, nil
}

func NewBuffer(path string) (*Buffer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	stats, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	size := stats.Size()

	return NewBufferWithSize(path, size)
}

func (b *Buffer) EnableWriteMode() {
	b.writeModeMux.Lock()
	b.writeMode = true
	b.writeModeMux.Unlock()
}

func (b *Buffer) DisableWriteMode() {
	b.writeModeMux.Lock()
	b.writeMode = false
	b.writeModeMux.Unlock()
}

func (b *Buffer) isWriteMode() bool {
	b.writeModeMux.RLock()
	defer b.writeModeMux.RUnlock()
	return b.writeMode
}

func (b *Buffer) syncLoop() {
	timer := time.NewTimer(b.syncPeriod)
	defer timer.Stop()

	for {
		select {
		case <-b.writeCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(b.syncPeriod)
		case <-timer.C:
			b.Sync()
			timer.Reset(b.syncPeriod)
		case <-b.stopCh:
			return
		}
	}
}

func (b *Buffer) markWrite() {
	select {
	case b.writeCh <- struct{}{}:
	default:
	}
}

func (b *Buffer) Write(offset int, input []byte) error {
	if b.isWriteMode() {
		_, err := b.file.WriteAt(input, int64(offset))
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	end := offset + len(input)
	if end > b.length {
		growBy := pageSize
		if end > b.length+growBy {
			growBy = end - b.length
		}
		if err := b.growUnlocked(growBy); err != nil {
			return err
		}
	}

	copy(b.data[offset:], input)
	b.markWrite()
	return nil
}

func (b *Buffer) Read(offset, size int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if offset+size > b.length {
		return nil, fmt.Errorf("read exceeds buffer size")
	}
	return b.data[offset : offset+size], nil
}

func (b *Buffer) growUnlocked(extra int) error {
	newLen := b.length + extra
	if err := b.file.Truncate(int64(newLen)); err != nil {
		return fmt.Errorf("failed to extend file: %w", err)
	}

	if err := unix.Msync(b.data, unix.MS_SYNC); err != nil {
		return fmt.Errorf("failed to sync before unmap: %w", err)
	}

	if err := unix.Munmap(b.data); err != nil {
		return fmt.Errorf("failed to unmap old data: %w", err)
	}

	newData, err := unix.Mmap(int(b.file.Fd()), 0, newLen, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to remap file: %w", err)
	}

	b.data = newData
	b.length = newLen
	return nil
}

func (b *Buffer) Grow(extra int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.growUnlocked(extra)
}

func (b *Buffer) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return unix.Msync(b.data, unix.MS_SYNC)
}

func (b *Buffer) Close() error {
	close(b.stopCh)

	b.mu.Lock()
	defer b.mu.Unlock()

	if err := unix.Munmap(b.data); err != nil {
		return err
	}
	return b.file.Close()
}
