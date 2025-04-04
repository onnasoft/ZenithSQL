package buffer

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type Buffer struct {
	data   []byte
	length int
	file   *os.File
}

func NewBuffer(path string, size int) (*Buffer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to truncate file: %w", err)
	}

	data, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &Buffer{
		data:   data,
		length: size,
		file:   f,
	}, nil
}

func (b *Buffer) Write(offset int, input []byte) error {
	if offset+len(input) > b.length {
		return fmt.Errorf("write exceeds buffer size")
	}
	copy(b.data[offset:], input)
	return nil
}

func (b *Buffer) Read(offset, size int) ([]byte, error) {
	if offset+size > b.length {
		return nil, fmt.Errorf("read exceeds buffer size")
	}
	return b.data[offset : offset+size], nil
}

func (b *Buffer) Sync() error {
	return unix.Msync(b.data, unix.MS_SYNC)
}

func (b *Buffer) Close() error {
	if err := unix.Munmap(b.data); err != nil {
		return err
	}
	return b.file.Close()
}
