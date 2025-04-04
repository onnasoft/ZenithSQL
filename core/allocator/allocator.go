package allocator

import (
	"fmt"
	"reflect"
	"sync"
)

type ZeroMemoryAllocator struct {
	maxSize       int
	allocatedSize int
	allocated     []interface{} // To keep track of allocated elements
	mutex         sync.Mutex
	generator     func() interface{}
}

// NewZeroMemoryAllocator creates a new ZeroMemoryAllocator
func NewZeroMemoryAllocator(maxSize int, generator func() interface{}) *ZeroMemoryAllocator {
	return &ZeroMemoryAllocator{
		maxSize:   maxSize,
		generator: generator,
		allocated: make([]interface{}, 0, maxSize),
	}
}

// Allocate will check if the allocator has memory available,
// and if not, will call the generator function to generate new elements.
func (a *ZeroMemoryAllocator) Allocate() (interface{}, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if a.allocatedSize < a.maxSize {
		// Memory is available, allocate new element
		element := a.generator()
		a.allocated = append(a.allocated, element)
		a.allocatedSize++
		return element, nil
	}

	// If no more memory is available, return an error
	return nil, fmt.Errorf("memory limit exceeded, cannot allocate more elements")
}

// Release will free the allocated element and reduce the memory size
func (a *ZeroMemoryAllocator) Release(element interface{}) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Compare elements safely using reflect.DeepEqual
	for i, e := range a.allocated {
		if reflect.DeepEqual(e, element) {
			// Remove the element from the allocated pool
			a.allocated = append(a.allocated[:i], a.allocated[i+1:]...)
			a.allocatedSize--
			return nil
		}
	}

	// If element not found
	return fmt.Errorf("element not found in allocated pool")
}

// Reset will free all allocated memory and reset the allocator
func (a *ZeroMemoryAllocator) Reset() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.allocated = make([]interface{}, 0, a.maxSize)
	a.allocatedSize = 0
}

// GetAllocatedSize returns the current number of allocated elements
func (a *ZeroMemoryAllocator) GetAllocatedSize() int {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.allocatedSize
}
