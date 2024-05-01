package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/parquet-go/parquet-go"
)

type (
	cliContext    = kong.Context
	parquetReader = parquet.Reader
)

var cli struct {
	Cat    CatCmd    `cmd:"" help:"print a parquet file"`
	Schema SchemaCmd `cmd:"" help:"print a parquet schema"`
	To     struct {
		Csv   ToCsvCmd   `cmd:"" help:"convert parquet to CSV"`
		JSON  ToJSONCmd  `cmd:"" help:"convert parquet to JSON"`
		JSONL ToJSONLCmd `cmd:"" help:"convert parquet to JSON-Lines"`
	} `cmd:"" help:"convert parquet to..."`
	Where WhereCmd `cmd:"" help:"filter a parquet file"`
}

func main() {
	k, err := run()
	k.FatalIfErrorf(err)
}

func run() (*kong.Context, error) {
	k := kong.Parse(&cli,
		kong.Name("parquetry"),
		kong.Description("Tooling for parquet files"),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)
	err := k.Run()
	return k, err
}

type FormatFlags struct {
	Go    bool `help:"format as go (default)" xor:"go,csv,json,jsonl" group:"Output Format:"`
	Csv   bool `help:"format as CSV" xor:"go,csv,json,jsonl" group:"Output Format:"`
	Json  bool `help:"format as JSON" xor:"go,csv,json,jsonl" group:"Output Format:"`
	Jsonl bool `help:"format as JSON-Lines" xor:"go,csv,json,jsonl" group:"Output Format:"`
}

type HeadTailFlags struct {
	Head int64 `help:"include first +n or skip first -n records" xor:"head,tail"`
	Tail int64 `help:"include last +n or skip last -n records" xor:"head,tail"`
}

func (f FormatFlags) newWriter(k *kong.Context, pq *parquetReader) RowWriteCloser {
	switch {
	case f.Csv:
		return newCSVWriter(k, pq)
	case f.Json:
		return newJSONWriter(k, pq)
	case f.Jsonl:
		return newJSONLWriter(k, pq)
	}
	return newGoWriter(k, pq)
}

type rowRange struct{ Start, Stop int64 }

func (f *HeadTailFlags) rowRange(pq *parquetReader) (rng rowRange, err error) {
	rows := pq.NumRows()
	rng.Stop = rows
	if f == nil {
		return
	}

	switch {
	case f.Head > 0:
		rng.Stop = f.Head
	case f.Head < 0:
		rng.Start = -f.Head
	case f.Tail > 0:
		rng.Start = rng.Stop - f.Tail
	case f.Tail < 0:
		rng.Stop = rng.Stop + f.Tail
	}
	if rng.Start < 0 {
		rng.Start = 0
	}
	if rng.Stop > rows {
		rng.Stop = rows
	}
	return
}

type SchemaCmd struct {
	Message  bool     `name:"message" help:"show as message definition" xor:"message,physical,logical"`
	Physical bool     `short:"p" help:"show physical types as a go structure" xor:"message,physical,logical"`
	Logical  bool     `short:"l" help:"show logical types as a go structure" xor:"message,physical,logical"`
	Files    []string `arg:"" name:"file" help:"parqet files to print" type:"existingfile"`
}

func (c SchemaCmd) Run(k *kong.Context) error {
	return eachFile(k, c.Files, func(k *kong.Context, name string) error {
		return withReader(name, func(pq *parquetReader) (err error) {
			switch {
			case c.Logical:
				_, err = fmt.Fprintln(k.Stdout, goLogicalType(pq.Schema()))
			case c.Physical:
				_, err = fmt.Fprintln(k.Stdout, pq.Schema().GoType())
			default:
				_, err = fmt.Fprintln(k.Stdout, pq.Schema())
			}
			return err
		})
	})
}

type RowWriteCloser interface {
	Write(v reflect.Value) error
	Close() error
}

type CatCmd struct {
	FormatFlags
	HeadTailFlags
	Files []string `arg:"" name:"file" help:"parqet files to print" type:"existingfile"`
}

type ToCsvCmd struct {
	HeadTailFlags
	Files []string `arg:"" name:"file" help:"parqet files to print" type:"existingfile"`
}
type ToJSONCmd struct {
	HeadTailFlags
	Files []string `arg:"" name:"file" help:"parqet files to print" type:"existingfile"`
}
type ToJSONLCmd struct {
	HeadTailFlags
	Files []string `arg:"" name:"file" help:"parqet files to print" type:"existingfile"`
}

type WhereCmd struct {
	FormatFlags
	Filter string   `arg:"" help:"include rows matching this expression"`
	Files  []string `arg:"" name:"file" help:"parqet files to print" type:"existingfile"`
}

func (c CatCmd) Run(k *kong.Context) error {
	return printAs(k, c.Files, &c.HeadTailFlags, c.newWriter, nil)
}

func (c ToCsvCmd) Run(k *kong.Context) error {
	return printAs(k, c.Files, &c.HeadTailFlags, newCSVWriter, nil)
}

func (c ToJSONCmd) Run(k *kong.Context) error {
	return printAs(k, c.Files, &c.HeadTailFlags, newJSONWriter, nil)
}

func (c ToJSONLCmd) Run(k *kong.Context) error {
	return printAs(k, c.Files, &c.HeadTailFlags, newJSONLWriter, nil)
}

func (c WhereCmd) Run(k *kong.Context) error {
	filter, err := ParseReflectFilter(c.Filter)
	if err != nil {
		return err
	}
	return printAs(k, c.Files, nil, c.newWriter, &filter)
}

func printAs(k *kong.Context, files []string, headtail *HeadTailFlags, newWriter func(*kong.Context, *parquetReader) RowWriteCloser, filter *ReflectFilter) error {
	return eachFile(k, files, func(k *kong.Context, name string) error {
		return withReader(name, func(pq *parquetReader) error {
			w := newWriter(k, pq)
			err := eachRow(k, pq, headtail, filter, w.Write)
			return errors.Join(err, w.Close())
		})
	})
}

type FilenameAction func(c *kong.Context, name string) error

func eachFile(k *kong.Context, files []string, do func(k *kong.Context, name string) error) error {
	for _, name := range files {
		if err := do(k, name); err != nil {
			return err
		}
	}
	return nil
}

func withReader(name string, do func(*parquetReader) error) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	pq := parquet.NewReader(f)
	defer pq.Close()

	return do(pq)
}

func eachRow(k *kong.Context, pq *parquetReader, headtail *HeadTailFlags, filter *ReflectFilter, do func(reflect.Value) error) error {
	rng, err := headtail.rowRange(pq)
	if err != nil {
		return err
	}
	rowType := goLogicalType(pq.Schema())
	v, z := reflect.New(rowType), reflect.Zero(rowType)

	if rng.Start > 0 {
		pq.SeekToRow(rng.Start)
	}
	for i := rng.Start; i < rng.Stop; i++ {
		v.Elem().Set(z)
		if err := pq.Read(v.Interface()); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if filter != nil {
			include, err := filter.Eval(v.Elem())
			if err != nil {
				return err
			}
			if !include {
				continue
			}
		}
		if err := do(v.Elem()); err != nil {
			return err
		}
	}
	return nil
}

// goLogicalType returns a more useful type than s.GoType()
//
// s.GoType returns a struct corresponding to the physical structure of the parquet file.
// However logical types can be handled better. In particular...
//
// - Logical maps should use map[K]V instead of a (nested) slice of key-value structs
// - Logical strings should use string instead of []uint8
func goLogicalType(s *parquet.Schema) reflect.Type {
	return reflect.StructOf(goLogicalTypeFields(s.Fields(), nil))
}

func goLogicalTypeFields(flds []parquet.Field, path []string) []reflect.StructField {
	sf := make([]reflect.StructField, len(flds))
	for i, pf := range flds {
		sf[i] = goLogicalTypeField(pf, append(path, pf.Name()))
	}
	return sf
}

func goLogicalTypeField(pf parquet.Field, path []string) reflect.StructField {
	name := pf.Name()
	title := strings.ToTitle(name[:1]) + name[1:]
	sf := reflect.StructField{
		Name: title,
		Type: pf.GoType(),
	}
	if name != title {
		sf.Tag = reflect.StructTag(fmt.Sprintf("json:%[1]q parquet:%[1]q", name))
	}

	if lt := pf.Type().LogicalType(); lt != nil {
		switch {
		case lt.UTF8 != nil:
			sf.Type = reflect.TypeFor[string]()
		case lt.Map != nil:
			kvs := pf.Fields()[0]
			mapfields := goLogicalTypeFields(kvs.Fields(), append(path, kvs.Name()))
			sf.Type = reflect.MapOf(mapfields[0].Type, mapfields[1].Type)
		case lt.Date != nil:
			sf.Type = reflect.TypeFor[Date]()
		case lt.Time != nil:
			switch {
			case lt.Time.Unit.Millis != nil:
				if lt.Time.IsAdjustedToUTC {
					sf.Type = reflect.TypeFor[TimeMilliUTC]()
				} else {
					sf.Type = reflect.TypeFor[TimeMilliLoc]()
				}
			case lt.Time.Unit.Micros != nil:
				if lt.Time.IsAdjustedToUTC {
					sf.Type = reflect.TypeFor[TimeMicroUTC]()
				} else {
					sf.Type = reflect.TypeFor[TimeMicroLoc]()
				}
			case lt.Time.Unit.Nanos != nil:
				if lt.Time.IsAdjustedToUTC {
					sf.Type = reflect.TypeFor[TimeNanoUTC]()
				} else {
					sf.Type = reflect.TypeFor[TimeNanoLoc]()
				}
			}
		case lt.Timestamp != nil:
			switch {
			case lt.Timestamp.Unit.Millis != nil:
				if lt.Timestamp.IsAdjustedToUTC {
					sf.Type = reflect.TypeFor[StampMilliUTC]()
				} else {
					sf.Type = reflect.TypeFor[StampMilliLoc]()
				}
			case lt.Timestamp.Unit.Micros != nil:
				if lt.Timestamp.IsAdjustedToUTC {
					sf.Type = reflect.TypeFor[StampMicroUTC]()
				} else {
					sf.Type = reflect.TypeFor[StampMicroLoc]()
				}
			case lt.Timestamp.Unit.Nanos != nil:
				if lt.Timestamp.IsAdjustedToUTC {
					sf.Type = reflect.TypeFor[StampNanoUTC]()
				} else {
					sf.Type = reflect.TypeFor[StampNanoLoc]()
				}
			}
		}
	} else {
		sf.Type = reflect.StructOf(goLogicalTypeFields(pf.Fields(), path))
	}
	return sf
}
