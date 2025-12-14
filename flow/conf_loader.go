package flow

import (
	"fmt"
	"io"
	"os"
)

type ConfReader interface {
	Read(name string) ([]byte, error)
}

var _ ConfReader = (*confReaderLocal)(nil)

type confReaderLocal struct {
}

func NewConfReaderLocal() ConfReader {
	return new(confReaderLocal)
}

func (c *confReaderLocal) Read(name string) (bytes []byte, err error) {
	confPath := "./conf/" + name
	file, e := os.Open(confPath)
	if e != nil {
		err = fmt.Errorf("open flow conf file failed: %w", err)
		return
	}

	defer func() {
		if e = file.Close(); e != nil {
			err = fmt.Errorf("flow conf close failed: %w", e)
		}
	}()
	bytes, err = io.ReadAll(file)
	return
}
