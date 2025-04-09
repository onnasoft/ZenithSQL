package buffer

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	maxGrowRetries = 3
)

type MMapFile struct {
	data     []byte
	file     *os.File
	size     int
	path     string
	pageSize int
	growMux  sync.RWMutex
}

func Open(path string, initialSize, pageSize int) (*MMapFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	stats, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if stats.Size() > 0 {
		initialSize = int(stats.Size())
	}

	// Ensure minimum size
	if initialSize < pageSize {
		initialSize = pageSize
	}

	if err := file.Truncate(int64(initialSize)); err != nil {
		file.Close()
		return nil, err
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, initialSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &MMapFile{
		data:     data,
		file:     file,
		size:     initialSize,
		pageSize: pageSize,
		path:     path,
	}, nil
}

func (m *MMapFile) CanWrite(offset, length int) bool {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	if offset < 0 || length <= 0 {
		return false
	}

	requiredEnd := offset + length
	if requiredEnd <= m.size {
		return true
	}

	// Try to grow if needed
	m.growMux.RUnlock()
	m.growMux.Lock()
	defer m.growMux.RLock()
	defer m.growMux.Unlock()

	return m.tryGrow(requiredEnd)
}

func (m *MMapFile) tryGrow(requiredSize int) bool {
	if requiredSize <= m.size {
		return true
	}

	// Calculate new size (round up to nearest pageSize multiple)
	newSize := ((requiredSize + m.pageSize - 1) / m.pageSize) * m.pageSize

	// Try growing the file
	for i := 0; i < maxGrowRetries; i++ {
		if err := m.growFile(newSize); err == nil {
			return true
		}
		// If we get ENOMEM, try with smaller increment
		newSize = m.size + m.pageSize
		if newSize >= requiredSize {
			break
		}
	}

	return false
}

func (m *MMapFile) growFile(newSize int) error {
	// Unmap existing mapping
	if err := syscall.Munmap(m.data); err != nil {
		return err
	}

	// Resize file
	if err := m.file.Truncate(int64(newSize)); err != nil {
		// Try to remap original size if grow fails
		syscall.Mmap(int(m.file.Fd()), 0, m.size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		return err
	}

	// Create new mapping
	data, err := syscall.Mmap(int(m.file.Fd()), 0, newSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		// Try to remap original size if new mapping fails
		syscall.Mmap(int(m.file.Fd()), 0, m.size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		return err
	}

	m.data = data
	m.size = newSize
	return nil
}

// Sync forces all data to disk
func (m *MMapFile) Sync() error {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	return m.syncRange(0, m.size)
}

// SyncRange synchronizes specific range to disk
func (m *MMapFile) SyncRange(offset, length int) error {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	// Validate range
	if offset < 0 || length <= 0 || offset+length > m.size {
		return fmt.Errorf("invalid range: offset %d, length %d (file size %d)",
			offset, length, m.size)
	}

	return m.syncRange(offset, length)
}

// syncRange internal implementation with bounds checking
func (m *MMapFile) syncRange(offset, length int) error {
	pageSize := syscall.Getpagesize()
	alignedOffset := offset - (offset % pageSize)
	alignedLength := length + (offset - alignedOffset)

	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&m.data[alignedOffset])),
		uintptr(alignedLength),
		syscall.MS_SYNC,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// AsyncSyncRange synchronizes range asynchronously
func (m *MMapFile) AsyncSyncRange(offset, length int) error {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	// Validate range
	if offset < 0 || length <= 0 || offset+length > m.size {
		return fmt.Errorf("invalid range: offset %d, length %d (file size %d)",
			offset, length, m.size)
	}

	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&m.data[offset])),
		uintptr(length),
		syscall.MS_ASYNC,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// Close unmaps and closes the file
func (m *MMapFile) Close() error {
	m.growMux.Lock()
	defer m.growMux.Unlock()

	if err := syscall.Munmap(m.data); err != nil {
		return err
	}
	return m.file.Close()
}

// Data returns the memory mapped bytes
func (m *MMapFile) Data() []byte {
	m.growMux.RLock()
	defer m.growMux.RUnlock()
	return m.data
}

// Size returns current mapped size
func (m *MMapFile) Size() int {
	m.growMux.RLock()
	defer m.growMux.RUnlock()
	return m.size
}
