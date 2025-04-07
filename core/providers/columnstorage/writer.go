package columnstorage

import (
	"errors"
	"fmt"
	"sync"
)

const (
	errWriterClosed  = "writer is closed"
	errIDZero        = "id cannot be zero"
	errIDNotFound    = "record with id %d not found in current transaction"
	errFieldNotFound = "column %s not found"
	errFieldInvalid  = "invalid value for column %s: %w"
)

const (
	statusByteOffset = 0 // First byte for status
	valueByteOffset  = 1 // Actual data starts after status byte
)

type Writer struct {
	columns   map[string]*Column
	pending   map[int64]struct{} // Using map for faster lookups
	mu        sync.Mutex
	closed    bool
	committed bool
}

func NewWriter(columns map[string]*Column) *Writer {
	return &Writer{
		columns: columns,
		pending: make(map[int64]struct{}),
	}
}

func (w *Writer) Write(values map[string]interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New(errWriterClosed)
	}

	// Validate ID first
	id, ok := values["id"].(int64)
	if !ok {
		return fmt.Errorf("missing or invalid id field")
	}
	if id == 0 {
		return errors.New(errIDZero)
	}
	id--

	// Check if record already exists
	if _, exists := w.pending[id]; exists {
		return fmt.Errorf("record with id %d already exists in this transaction", id+1)
	}

	// Validate all fields
	for name, value := range values {
		col, ok := w.columns[name]
		if !ok {
			return fmt.Errorf(errFieldNotFound, name)
		}
		if err := col.isValid(value); err != nil {
			return fmt.Errorf(errFieldInvalid, name, err)
		}
	}

	// Write each field
	for name, value := range values {
		if err := w.writeFieldInternal(id, name, value); err != nil {
			return err
		}
	}

	w.pending[id] = struct{}{}
	return nil
}

func (w *Writer) writeFieldInternal(id int64, name string, value interface{}) error {
	col := w.columns[name]
	recordLength := col.Length + 2 // +1 for status byte
	offset := id * int64(recordLength)

	if !col.MMapFile.CanWrite(int(offset), recordLength) {
		return fmt.Errorf("record exceeds buffer capacity for column %s", name)
	}

	data := col.MMapFile.Data()[offset : offset+int64(recordLength)]
	data[statusByteOffset] = 1 // Mark as set

	if err := col.write(data[valueByteOffset:], value); err != nil {
		return fmt.Errorf("error writing value for column %s: %w", name, err)
	}
	data[col.Length+1] = '\n'

	return nil
}

func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// Sync all columns
	for name, col := range w.columns {
		if err := col.MMapFile.Sync(); err != nil {
			return fmt.Errorf("failed to flush column %s: %w", name, err)
		}
	}
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	var err error
	if !w.committed {
		err = w.rollbackInternal()
	}

	// Clear pending regardless of commit state
	w.pending = make(map[int64]struct{})
	return err
}

func (w *Writer) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New(errWriterClosed)
	}

	// Sync only the modified ranges for each column
	for id := range w.pending {
		for name, col := range w.columns {
			recordLength := col.Length + 2
			offset := (id) * int64(recordLength)
			syncLength := recordLength

			// Check bounds
			if offset+int64(syncLength) > int64(len(col.MMapFile.Data())) {
				continue
			}

			// Sync the modified range for this column
			if err := col.MMapFile.Sync(); err != nil {
				return fmt.Errorf("failed to sync column %s at offset %d: %w",
					name, offset, err)
			}
		}
	}

	w.committed = true
	w.pending = make(map[int64]struct{})
	return nil
}

func (w *Writer) Rollback() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	return w.rollbackInternal()
}

func (w *Writer) rollbackInternal() error {
	for id := range w.pending {
		for _, col := range w.columns {
			recordLength := col.Length + 2 // +2 for status and newline
			offset := id * int64(recordLength)

			// Check bounds before writing
			if offset < int64(len(col.MMapFile.Data())) {
				col.MMapFile.Data()[offset] = 0 // Reset status byte
			}
		}
	}

	w.pending = make(map[int64]struct{})
	return nil
}
