package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/parquet-go/parquet-go"
)

func newCSVWriter(k *cliContext, pq *parquetReader) RowWriteCloser {
	return &csvWriter{w: k.Stdout, s: pq.Schema()}
}

type csvWriter struct {
	w    io.Writer
	c    *csv.Writer
	s    *parquet.Schema
	vals []string
	err  error
}

func (w *csvWriter) Write(v reflect.Value) error {
	if w.err != nil {
		return w.err
	}
	if w.c == nil {
		w.c = csv.NewWriter(w.w)
		fields := w.s.Fields()
		hdr := make([]string, len(fields))
		for i, f := range fields {
			hdr[i] = f.Name()
		}
		if w.err = w.c.Write(hdr); w.err != nil {
			return w.err
		}
		w.vals = make([]string, len(fields))
	}

	if w.vals != nil {
		for i := range w.s.Fields() {
			switch v := v.Field(i); v.Kind() {
			case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8,
				reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8,
				reflect.Float64, reflect.Float32, reflect.String:
				w.vals[i] = fmt.Sprint(v.Interface())
			default:
				var b []byte
				b, w.err = json.Marshal(v.Interface())
				if w.err != nil {
					return w.err
				}
				w.vals[i] = string(b)
			}
		}
		w.err = w.c.Write(w.vals)
	}
	return w.err
}

func (w *csvWriter) Close() error {
	w.c.Flush()
	return errors.Join(w.err, w.c.Error())
}
