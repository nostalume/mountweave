package ftp

import (
	"bufio"
	"io"
)

// TYPE A uses NVT CRLF/CR-NUL on the wire and LF/literal CR in stored content.
type asciiReader struct{ input *bufio.Reader }

func (r *asciiReader) Read(p []byte) (int, error) {
	for i := range p {
		b, err := r.input.ReadByte()
		if err != nil {
			return i, err
		}
		if b == '\r' {
			next, err := r.input.ReadByte()
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			if err != nil {
				return i, err
			}
			switch next {
			case '\n':
				b = '\n'
			case 0:
				b = '\r'
			default:
				return i, io.ErrUnexpectedEOF
			}
		}
		p[i] = b
	}
	return len(p), nil
}

type asciiWriter struct {
	output io.Writer
	buffer []byte
	limit  int
}

func (w *asciiWriter) Write(p []byte) (int, error) {
	consumed := 0
	for len(p) > 0 {
		block := p[:min(len(p), w.limit)]
		w.buffer = w.buffer[:0]
		for _, b := range block {
			switch b {
			case '\n':
				w.buffer = append(w.buffer, '\r', '\n')
			case '\r':
				w.buffer = append(w.buffer, '\r', 0)
			default:
				w.buffer = append(w.buffer, b)
			}
		}
		for data := w.buffer; len(data) > 0; {
			n, err := w.output.Write(data)
			if err != nil {
				return consumed, err
			}
			if n == 0 {
				return consumed, io.ErrShortWrite
			}
			data = data[n:]
		}
		consumed += len(block)
		p = p[len(block):]
	}
	return consumed, nil
}
