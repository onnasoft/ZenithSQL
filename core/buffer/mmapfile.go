package buffer

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	maxGrowRetries  = 3
	errInvalidRange = "invalid range: offset %d, length %d (file size %d)"
)

type MMapFile struct {
	data     []byte
	file     *os.File
	size     int
	path     string
	pageSize int
	growMux  sync.RWMutex

	// Views management
	views    map[*viewInfo]struct{}
	viewsMux sync.Mutex
}

type viewInfo struct {
	data     []byte
	size     int
	refCount int
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
		views:    make(map[*viewInfo]struct{}),
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

func (m *MMapFile) AllocateView() ([]byte, error) {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	view, err := syscall.Mmap(int(m.file.Fd()), 0, m.size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap read view: %w", err)
	}

	info := &viewInfo{
		data:     view,
		size:     m.size,
		refCount: 1,
	}

	m.viewsMux.Lock()
	m.views[info] = struct{}{}
	m.viewsMux.Unlock()

	return view, nil
}

func (m *MMapFile) FreeView(view []byte) {
	m.viewsMux.Lock()
	defer m.viewsMux.Unlock()

	for info := range m.views {
		if &info.data[0] == &view[0] {
			info.refCount--
			if info.refCount == 0 {
				syscall.Munmap(info.data)
				delete(m.views, info)
			}
			return
		}
	}
}

func (m *MMapFile) tryGrow(requiredSize int) bool {
	if requiredSize <= m.size {
		return true
	}

	newSize := ((requiredSize + m.pageSize - 1) / m.pageSize) * m.pageSize

	for i := 0; i < maxGrowRetries; i++ {
		if err := m.growFile(newSize); err == nil {
			return true
		}

		newSize = m.size + m.pageSize
		if newSize >= requiredSize {
			break
		}
	}

	return false
}

func (m *MMapFile) growFile(newSize int) error {
	if err := syscall.Munmap(m.data); err != nil {
		return err
	}

	if err := m.file.Truncate(int64(newSize)); err != nil {
		syscall.Mmap(int(m.file.Fd()), 0, m.size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		return err
	}

	data, err := syscall.Mmap(int(m.file.Fd()), 0, newSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Mmap(int(m.file.Fd()), 0, m.size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		return err
	}

	m.data = data
	m.size = newSize
	return nil
}

func (m *MMapFile) Sync() error {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	return m.syncRange(0, m.size)
}

func (m *MMapFile) SyncRange(offset, length int) error {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	if offset < 0 || length <= 0 || offset+length > m.size {
		return fmt.Errorf(errInvalidRange,
			offset, length, m.size)
	}

	return m.syncRange(offset, length)
}

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

func (m *MMapFile) AsyncSyncRange(offset, length int) error {
	m.growMux.RLock()
	defer m.growMux.RUnlock()

	// Validate range
	if offset < 0 || length <= 0 || offset+length > m.size {
		return fmt.Errorf(errInvalidRange,
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

func (m *MMapFile) Close() error {
	m.growMux.Lock()
	defer m.growMux.Unlock()

	m.viewsMux.Lock()
	for info := range m.views {
		syscall.Munmap(info.data)
		delete(m.views, info)
	}
	m.viewsMux.Unlock()

	if err := syscall.Munmap(m.data); err != nil {
		return err
	}
	return m.file.Close()
}

func (m *MMapFile) ReadAt(offset int, length int) ([]byte, error) {
	if offset < 0 || length <= 0 || offset+length > m.size {
		return nil, fmt.Errorf(errInvalidRange,
			offset, length, m.size)
	}

	return m.data[offset : offset+length], nil
}

func (m *MMapFile) Data() []byte {
	m.growMux.RLock()
	defer m.growMux.RUnlock()
	return m.data
}

func (m *MMapFile) Size() int {
	m.growMux.RLock()
	defer m.growMux.RUnlock()
	return m.size
}
