package main

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
)

type jsonWriter struct {
	w      io.Writer
	b      bytes.Buffer
	e      *json.Encoder
	err    error
	prefix []byte
}

func (w *jsonWriter) Write(v reflect.Value) error {
	if w.err != nil {
		return w.err
	}

	if w.e == nil {
		w.e = json.NewEncoder(&w.b)
		w.e.SetEscapeHTML(false)
		w.prefix = []byte("[\n  ")
	}

	if w.err = w.e.Encode(v.Interface()); w.err != nil {
		return w.err
	}

	j := bytes.TrimSuffix(w.b.Bytes(), []byte{'\n'})
	defer w.b.Reset()
	if _, w.err = w.w.Write(w.prefix); w.err != nil {
		return w.err
	}
	w.prefix[0] = ','
	_, w.err = w.w.Write(j)
	return w.err
}

func (w *jsonWriter) Close() error {
	if w.e != nil {
		w.e = nil
		_, w.err = w.w.Write([]byte("\n]\n"))
	} else {
		_, w.err = w.w.Write([]byte("[]"))
	}
	return w.err
}
