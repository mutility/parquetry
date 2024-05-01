package main

import (
	"encoding/json"
	"io"
	"reflect"
)

func newJSONLWriter(c *cliContext, pq *parquetReader) RowWriteCloser {
	return &jsonlWriter{w: c.Stdout}
}

type jsonlWriter struct {
	w   io.Writer
	e   *json.Encoder
	err error
}

func (w *jsonlWriter) Write(v reflect.Value) error {
	if w.e == nil {
		w.e = json.NewEncoder(w.w)
		w.e.SetEscapeHTML(false)
	}
	w.err = w.e.Encode(v.Interface())
	return w.err
}

func (w *jsonlWriter) Close() error {
	return w.err
}
