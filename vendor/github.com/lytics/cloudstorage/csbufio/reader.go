package csbufio

import (
	"bufio"
	"io"
	"os"
)

func OpenReader(name string) (io.ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return NewReader(f), nil
}

func NewReader(rc io.ReadCloser) io.ReadCloser {
	return bufReadCloser{bufio.NewReader(rc), rc}
}

type bufReadCloser struct {
	io.Reader
	c io.Closer
}

func (bc bufReadCloser) Close() error { return bc.c.Close() }
