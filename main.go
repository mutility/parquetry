package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/parquet-go/parquet-go"
)

type parquetReader = parquet.Reader

var cli struct {
	Cat     CatCmd     `cmd:"" help:"Print a parquet file"`
	Head    HeadCmd    `cmd:"" help:"Print (or skip) the beginning of a parquet file"`
	Tail    TailCmd    `cmd:"" help:"Print (or skip) the ending of a parquet file"`
	Schema  SchemaCmd  `cmd:"" help:"Print a parquet schema"`
	To      ToCmd      `cmd:"" help:"Convert parquet to..."`
	Where   WhereCmd   `cmd:"" help:"Filter a parquet file"`
	Reshape ReshapeCmd `cmd:"" help:"Reshape a parquet file"`
}

func main() {
	err := run()
	if err != nil {
		os.Exit(1)
	}
}

func run() error {
	k := kong.Parse(&cli,
		kong.Name("parquetry"),
		kong.Description("Tooling for parquet files"),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
		kong.UsageOnError(),
	)
	if err := k.Run(); err != nil {
		k.Errorf("%v", err)
		return err
	}
	return nil
}

type SchemaCmd struct {
	Format string   `short:"f" default:"message" help:"Output schema as message or logical/physical struct" enum:"message,m,logical,l,physical,p"`
	Files  []string `arg:"" name:"file" help:"Parquet files" type:"file"`
}

func (c SchemaCmd) Run(k *kong.Context) error {
	return eachFile(k, c.Files, func(k *kong.Context, name string) error {
		return withReader(name, func(pq *parquetReader) (err error) {
			switch c.Format {
			case "message":
				_, err = fmt.Fprintln(k.Stdout, pq.Schema())
			case "physical", "p":
				_, err = fmt.Fprintln(k.Stdout, pq.Schema().GoType())
			case "logical", "l":
				_, err = fmt.Fprintln(k.Stdout, goLogicalType(pq.Schema()))
			}
			return err
		})
	})
}

type WriteCloser interface {
	Write(v reflect.Value) error
	Close() error
}

type CatCmd struct {
	Format string   `short:"f" default:"go" enum:"go,csv,json,jsonl" help:"Output as go, csv, json, or jsonl"`
	Head   int64    `placeholder:"n|-n" help:"Include first n or skip first -n records" xor:"head,tail"`
	Tail   int64    `placeholder:"n|-n" help:"Include last n or skip last -n records" xor:"head,tail"`
	Files  []string `arg:"" name:"file" help:"Parquet files" type:"file"`
}

type HeadCmd struct {
	Format string `short:"f" default:"go" enum:"go,csv,json,jsonl" help:"Output as go, csv, json, or jsonl"`
	Head   int64  `arg:"" name:"rows" placeholder:"n|-n" help:"Include first n or skip first -n records"`
	File   string `arg:"" help:"Parquet file" type:"file"`
}

type TailCmd struct {
	Format string `short:"f" default:"go" enum:"go,csv,json,jsonl" help:"Output as go, csv, json, or jsonl"`
	Tail   int64  `arg:"" name:"rows" placeholder:"n|-n" help:"Include last n or skip last -n records"`
	File   string `arg:"" help:"Parquet file" type:"file"`
}

type ToCmd struct {
	Format string   `arg:"" enum:"go,csv,json,jsonl" help:"Output as go, csv, json, or jsonl"`
	Head   int64    `placeholder:"n|-n" help:"Include first n or skip first -n records" xor:"head,tail"`
	Tail   int64    `placeholder:"n|-n" help:"Include last n or skip last -n records" xor:"head,tail"`
	Files  []string `arg:"" name:"file" help:"Parquet files" type:"file"`
}

type WhereCmd struct {
	Format string   `short:"f" default:"go" enum:"go,csv,json,jsonl" help:"Output as go, csv, json, or jsonl"`
	Shape  string   `short:"x" help:"Transform into specified shape"`
	Filter string   `arg:"" help:"Include rows matching this expression"`
	Files  []string `arg:"" name:"file" help:"Parquet files" type:"file"`
}

type ReshapeCmd struct {
	Format string   `short:"f" default:"go" enum:"go,csv,json,jsonl" help:"Output as go, csv, json, or jsonl"`
	Filter string   `short:"m" help:"Include rows matching this expression"`
	Shape  string   `arg:"" help:"Transform into specified shape"`
	Files  []string `arg:"" name:"file" help:"Parquet files" type:"file"`
}

func (WhereCmd) Help() string {
	return `
Specify the desired filter per the Flexera filter language:

Compare a dotted name to literals true/false/null, integers, dates, times, or strings.
Values must match the type of the data to which they are compared.

Comparisons include eq (equality), ne (inequality), lt (less), gt (greater),
le (less or equal), ge (greater or equal), co (contains), nc (not contains),
in (member of), ni (not member of).
Group comparisons with optional (), and tweak with a leading NOT, adjoining AND or OR.

Examples:
  - a eq true OR b eq false
  - a ne null
  - a lt 'b'
  - a le '2024-01-01T01:01:01.111Z'
  - a gt '01:01:01.111Z'
  - a ge '2024-01-01' AND a lt '2025-01-01'
  - a co 'e' AND a nc 'f'
  - a in [1, 2, 3]
  - a ni ['d', 'e', 'f']
  - NOT((a eq 1 AND b eq 2) OR (a eq 2 AND b eq 1))
`
}

func (ReshapeCmd) Help() string {
	return `
Specify the desired shape as a list of fields and groups.
  - Fields are a dotted name like a.b.c specifying their source
  - Groups are a parenthesized list of fields and groups
  - Groups must, and fields may specify a name with: AS $name

For example, if the source has fields A,B,C,D,E,F,G:
  - 'A,B,C' will take the first three columns
  - 'G,F,E' will take the last three columns in reverse order
  - 'A, A AS B' will output column A with names A and B
  - '(A,C,E,G) AS Odd, (B,D,F) AS Even' will turn subsets into new groups

If the source has a group Person with fields Name and Age:
  - '(Person.Name, Person.Age) as Person' will mimic the original layout
  - 'Person.Name, Person.Age' will flatten the nested group into Name,Age
  `
}

func (c CatCmd) Run(k *kong.Context) error {
	return cat{"", "", c.Files, c.Head, c.Tail, c.Format}.Run(k)
}

func (c HeadCmd) Run(k *kong.Context) error {
	return cat{"", "", []string{c.File}, c.Head, 0, c.Format}.Run(k)
}

func (c TailCmd) Run(k *kong.Context) error {
	return cat{"", "", []string{c.File}, 0, c.Tail, c.Format}.Run(k)
}

func (c ToCmd) Run(k *kong.Context) error {
	return cat{"", "", c.Files, c.Head, c.Tail, c.Format}.Run(k)
}

func (c WhereCmd) Run(k *kong.Context) error {
	return cat{c.Filter, c.Shape, c.Files, 0, 0, c.Format}.Run(k)
}

func (c ReshapeCmd) Run(k *kong.Context) error {
	return cat{c.Filter, c.Shape, c.Files, 0, 0, c.Format}.Run(k)
}

type cat struct {
	Filter     string
	Shape      string
	Files      []string
	Head, Tail int64
	Format     string
}

func (c cat) Run(k *kong.Context) error {
	return eachFile(k, c.Files, func(k *kong.Context, name string) error {
		return withReader(name, func(pq *parquetReader) error {
			return withWriter(c.Format, k.Stdout, func(write WriteFunc) error {
				return c.eachRow(k, pq, c.filter(c.reshape(pq, write)))
			})
		})
	})
}

func withWriter(format string, w io.Writer, do func(WriteFunc) error) error {
	switch format {
	case "go":
		return do(func(v reflect.Value) error {
			_, err := fmt.Fprintf(w, "%+v\n", v)
			return err
		})
	case "csv":
		cw := &csvWriter{w: w}
		err := do(cw.Write)
		return errors.Join(err, cw.Close())
	case "json":
		jw := &jsonWriter{w: w}
		err := do(jw.Write)
		return errors.Join(err, jw.Close())
	case "jsonl":
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		return do(func(v reflect.Value) error { return enc.Encode(v.Interface()) })
	}
	return fmt.Errorf("format %s: %w", format, errors.ErrUnsupported)
}

type WriteFunc func(reflect.Value) error

func (c cat) filter(w WriteFunc) WriteFunc {
	if c.Filter == "" {
		return w
	}
	filter, err := ParseReflectFilter(c.Filter)
	if err != nil {
		return func(reflect.Value) error { return err }
	}
	return func(v reflect.Value) error {
		if include, err := filter.Eval(v); err != nil {
			return err
		} else if include {
			return w(v)
		}
		return nil
	}
}

func (c cat) reshape(pq *parquetReader, w WriteFunc) WriteFunc {
	return reshape(c.Shape, pq, w)
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

func (c cat) eachRow(k *kong.Context, pq *parquetReader, do WriteFunc) error {
	rows := pq.NumRows()
	var start, stop int64 = 0, rows

	switch {
	case c.Head != 0 && c.Tail != 0:
		return fmt.Errorf("only one of --head and --tail may be provided")
	case c.Head > 0:
		stop = c.Head
	case c.Head < 0:
		start = -c.Head
	case c.Tail > 0:
		start = rows - c.Tail
	case c.Tail < 0:
		stop = rows + c.Tail
	}
	if start < 0 {
		start = 0
	}
	if stop > rows {
		stop = rows
	}
	rowType := goLogicalType(pq.Schema())
	v, z := reflect.New(rowType), reflect.Zero(rowType)

	if start > 0 {
		pq.SeekToRow(start)
	}
	for i := start; i < stop; i++ {
		v.Elem().Set(z)
		if err := pq.Read(v.Interface()); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
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
