package main

import (
	"reflect"
	"strings"
	"sync"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

func reshape(shape string, pq *parquetReader, w WriteFunc) WriteFunc {
	if shape == "" {
		return w
	}
	reshape, err := ParseShape(shape, goLogicalType(pq.Schema()))
	if err != nil {
		return func(reflect.Value) error { return err }
	}
	return func(v reflect.Value) error {
		r, err := reshape.Eval(v)
		if err != nil {
			return err
		}
		return w(r)
	}
}

var (
	reshapeParserOnce sync.Once
	reshapeParser     *participle.Parser[reFields]
)

func ParseShape(shape string, t reflect.Type) (*reshaper, error) {
	reshapeParserOnce.Do(func() {
		reshapeParser = participle.MustBuild[reFields](
			participle.Union[reValue](reStruct{}, reField{}),
			participle.Lexer(lexer.MustSimple([]lexer.SimpleRule{
				{Name: "As", Pattern: `[Aa][Ss]`},
				{Name: "Ident", Pattern: `[a-zA-Z_]+`},
				{Name: "Punct", Pattern: `[(),.]`},
				{Name: "whitespace", Pattern: `[ \t]+`},
			})),
		)
	})

	shaper, err := reshapeParser.ParseString("shape", shape)
	if err != nil {
		return nil, err
	}
	return &reshaper{shaper.Fields, t}, nil
}

type reshaper struct {
	fields []reValue
	source reflect.Type
}

func (r *reshaper) Type() reflect.Type {
	return reStructFor(r.fields, r.source)
}

func (r *reshaper) Eval(v reflect.Value) (reflect.Value, error) {
	return reStructOf(r.fields, v), nil
}

type reValue interface {
	reValue()
	String() string
	Eval(reflect.Value) reflect.Value
	Type(reflect.Type) reflect.Type
}

type reField struct {
	Source string `parser:"@Ident ( @'.' @Ident )*"`
	Name   string `parser:"( As @Ident )?"`
}
type reStruct struct {
	Fields []reValue `parser:"'(' @@ ( ',' @@ )* ')'"`
	Name   string    `parser:"( As @Ident )"`
}

type reFields struct {
	Fields []reValue `parser:"@@ ( ',' @@ )*"`
}

func (reField) reValue()  {}
func (reStruct) reValue() {}

func (f reField) String() string {
	if f.Name != "" {
		return f.Source + " AS " + f.Name
	}
	return f.Source
}

func (s reStruct) String() string {
	sf := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		sf[i] = f.String()
	}
	return "<" + strings.Join(sf, ", ") + ">"
}

func (f reField) Type(t reflect.Type) reflect.Type {
	return reTypeOf(t, f.Source)
}

func (f reField) Eval(v reflect.Value) reflect.Value {
	return reValueOf(v, f.Source)
}

func (s reStruct) Type(t reflect.Type) reflect.Type {
	return reStructFor(s.Fields, t)
}

func reStructFor(fields []reValue, t reflect.Type) reflect.Type {
	sf := make([]reflect.StructField, len(fields))
	for i, f := range fields {
		sf[i].Name = reNameOf(f)
		sf[i].Type = f.Type(t)
	}
	return reflect.StructOf(sf)
}

func (s reStruct) Eval(v reflect.Value) reflect.Value {
	return reStructOf(s.Fields, v)
}

func reStructOf(fields []reValue, v reflect.Value) reflect.Value {
	e := reflect.New(reStructFor(fields, v.Type())).Elem()
	for i, f := range fields {
		e.Field(i).Set(f.Eval(v))
	}
	return e
}

func reNameOf(v reValue) string {
	switch v := v.(type) {
	case reField:
		if v.Name != "" {
			return v.Name
		}
		return reLastDot(v.Source)
	case reStruct:
		return v.Name
	}
	return ""
}

func reTypeOf(t reflect.Type, source string) reflect.Type {
	if source == "" {
		return t
	}
	switch t.Kind() {
	case reflect.Struct:
		step, rest, _ := strings.Cut(source, ".")
		tf, ok := t.FieldByName(step)
		if !ok {
			return tf.Type
		}
		return reTypeOf(tf.Type, rest)
	default:
		return t
	}
}

func reValueOf(v reflect.Value, source string) reflect.Value {
	if source == "" {
		return v
	}
	switch v.Kind() {
	case reflect.Struct:
		step, rest, _ := strings.Cut(source, ".")
		return reValueOf(v.FieldByName(step), rest)
	default:
		return v
	}
}

func reLastDot(source string) string {
	if n := strings.LastIndexByte(source, '.'); n > 0 {
		return source[n+1:]
	}
	return source
}
