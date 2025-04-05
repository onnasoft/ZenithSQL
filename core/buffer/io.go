package buffer

type ReadWriter interface {
	Write(p []byte) (int, error)
	Read(p []byte) (int, error)
	ReadAt(p []byte, offset int) (int, error)
	Offset() int
	Seek(offset int)
}
