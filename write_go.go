package main

import (
	"fmt"
	"io"
	"reflect"
)

func newGoWriter(c *cliContext, pq *parquetReader) RowWriteCloser {
	return &goWriter{c.App.Writer}
}

type goWriter struct {
	w io.Writer
}

func (w *goWriter) Write(v reflect.Value) error {
	_, err := fmt.Fprintf(w.w, "%+v\n", v.Interface())
	return err
}

func (w *goWriter) Close() error {
	return nil
}
