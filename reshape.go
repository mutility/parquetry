package main

import (
	"reflect"
	"strings"
	"sync"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

func reshapeWrite(shape Shape, rowType reflect.Type, w WriteFunc) (WriteFunc, error) {
	if shape == "" {
		return w, nil
	}
	reshape, err := ParseShape(shape, rowType)
	if err != nil {
		return w, err
	}
	return func(v reflect.Value) error {
		r, err := reshape.Eval(v)
		if err != nil {
			return err
		}
		return w(r)
	}, nil
}

var (
	reshapeParserOnce sync.Once
	reshapeParser     *participle.Parser[reFields]
)

func ParseShape(shape Shape, t reflect.Type) (*reshaper, error) {
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

	shaper, err := reshapeParser.ParseString("shape", string(shape))
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
		name := reNameOf(f)
		sf[i].Name = name
		sf[i].Type = f.Type(t)
		if len(name) > 0 && name[:1] != strings.ToUpper(name[:1]) {
			sf[i].Name = strings.ToUpper(name[:1]) + name[1:]
			sf[i].Tag = reflect.StructTag(`parquet:"` + name + `" json:"` + name + `"`)
		}
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
		tf, ok := reTypeField(t, step)
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
		return reValueOf(reValueField(v, step), rest)
	default:
		return v
	}
}

func reTypeField(t reflect.Type, name string) (reflect.StructField, bool) {
	for f, F := 0, t.NumField(); f < F; f++ {
		fld := t.Field(f)
		if n, _, _ := strings.Cut(fld.Tag.Get("parquet"), ","); n != "" {
			if n == name {
				return fld, true
			}
		} else if fld.Name == name {
			return fld, true
		}
	}
	return reflect.StructField{}, false
}

func reValueField(v reflect.Value, name string) reflect.Value {
	for f, F := 0, v.NumField(); f < F; f++ {
		fld := v.Type().Field(f)
		if n, _, _ := strings.Cut(fld.Tag.Get("parquet"), ","); n != "" {
			if n == name {
				return v.Field(f)
			}
		} else if fld.Name == name {
			return v.Field(f)
		}
	}
	return reflect.Value{}
}

func reLastDot(source string) string {
	if name := strings.LastIndexByte(source, '.'); name > 0 {
		return source[name+1:]
	}
	return source
}
