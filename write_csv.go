package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type csvWriter struct {
	w    io.Writer
	c    *csv.Writer
	vals []string
	err  error
}

func (w *csvWriter) Write(v reflect.Value) error {
	if w.err != nil {
		return w.err
	}
	if w.c == nil {
		w.c = csv.NewWriter(w.w)
		t := v.Type()
		if t.Kind() != reflect.Struct {
			return fmt.Errorf("csv: unsupported output %s", t)
		}
		hdr := make([]string, t.NumField())
		for i := range hdr {
			f := t.Field(i)
			hdr[i] = f.Name
			if n, _, _ := strings.Cut(f.Tag.Get("parquet"), ","); n != "" {
				hdr[i] = n
			}
		}
		if w.err = w.c.Write(hdr); w.err != nil {
			return w.err
		}
		w.vals = make([]string, t.NumField())
	}

	if w.vals != nil {
		for i := range w.vals {
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
