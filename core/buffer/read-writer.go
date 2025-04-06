package buffer

type MReadWriter struct {
	buf    *Buffer
	offset int
}

func NewReadWriter(buf *Buffer) ReadWriter {
	return &MReadWriter{buf: buf}
}

func (rw *MReadWriter) Write(p []byte) (int, error) {
	err := rw.buf.Write(rw.offset, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (rw *MReadWriter) Read(p []byte) (int, error) {
	data, err := rw.buf.Read(rw.offset, len(p))
	if err != nil {
		return 0, err
	}
	copy(p, data)
	return len(data), nil
}

func (rw *MReadWriter) ReadAt(p []byte, offset int) (int, error) {
	data, err := rw.buf.Read(rw.offset+offset, len(p))
	if err != nil {
		return 0, err
	}
	copy(p, data)
	return len(data), nil
}

func (rw *MReadWriter) Offset() int {
	return rw.offset
}

func (rw *MReadWriter) Seek(offset int) {
	rw.offset = offset
}
