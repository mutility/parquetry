package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/mutility/parquetry/call"
)

type parquetReader = parquet.Reader

func main() {
	err := run()
	if err != nil {
		os.Exit(1)
	}
}

func run() error {
	p := call.Program("parquetry", "Tooling for parquet files")
	catCmd := p.Command("cat", "Print a parquet file")
	headCmd := p.Command("head", "Print (or skip) the beginning of a parquet file")
	tailCmd := p.Command("tail", "Print (or skip) the ending of a parquet file")
	schemaCmd := p.Command("schema", "Print a parquet schema")
	toCmd := p.Command("to", "Convert parquet to...")
	whereCmd := p.Command("where", "Filter a parquet file").Detail(filterHelp)
	shapeCmd := p.Command("reshape", "Reshape a parquet file").Detail(shapeHelp)

	out := call.Enumerated("format", "Output as go, csv, json, or jsonl", "go", "csv", "json", "jsonl").Default("go", "")
	head := call.Option[int64]("head", "Include first n or skip first -n rows", call.DashNumber).Hint("n|-n")
	tail := call.Option[int64]("tail", "Include last n or skip last -n rows", call.DashNumber).Hint("n|-n")
	filter := call.Option[string]("filter", "Include rows matching this expression", call.SeeCmd(whereCmd)).Hint("FILTER")
	shape := call.Option[string]("shape", "Transform rows into specified shape", call.SeeCmd(shapeCmd)).Hint("SHAPE")

	out.FlagsOn("-f|--format", catCmd, headCmd, tailCmd, whereCmd, shapeCmd).PosOn("", toCmd)
	head.FlagOn(catCmd, toCmd).PosOn("rows", headCmd)
	tail.FlagOn(catCmd, toCmd).PosOn("rows", tailCmd)
	filter.FlagsOn("-m|--filter", shapeCmd).PosOn("", whereCmd)
	shape.FlagsOn("-x|--shape", whereCmd).PosOn("", shapeCmd)

	schemaFmt := call.Enumerated[string]("format", "Output schema as message or logical/physical struct",
		"message", "m", "logical", "l", "physical", "p").Default("message", "")
	schemaFmt.FlagsOn("-f|--format", schemaCmd)

	file := call.Option[string]("file", "Parquet file", call.DashStdio).PosOn("", headCmd, tailCmd)
	files := call.Multi[string]("file", "Parquet files", call.DashStdio).PosOn("", catCmd, schemaCmd, toCmd, whereCmd, shapeCmd)
	noFilter, noShape := call.Hardcoded(""), call.Hardcoded("")
	catCmd.Runs(call.Handler6(printFile, out, head, tail, noFilter, noShape, files))
	headCmd.Runs(call.Handler6(printFile, out, head, call.Hardcoded[int64](0), noFilter, noShape, call.Singleton(file)))
	tailCmd.Runs(call.Handler6(printFile, out, call.Hardcoded[int64](0), tail, noFilter, noShape, call.Singleton(file)))
	toCmd.Runs(call.Handler6(printFile, out, head, tail, noFilter, noShape, files))
	whereCmd.Runs(call.Handler6(printFile, out, head, tail, filter, shape, files))
	shapeCmd.Runs(call.Handler6(printFile, out, head, tail, filter, shape, files))
	schemaCmd.Runs(call.Handler2(printSchema, schemaFmt, files))

	cmd, err := p.Parse(os.Args)
	if err != nil {
		p.ReportError(err)
		return err
	}
	err = p.RunCommand(context.Background(), cmd)
	if err != nil {
		p.ReportError(err)
	}
	return err
}

func printSchema(ctx context.Context, e call.Environ, format string, files []string) error {
	return eachFile(files, func(name string) error {
		return withReader(name, func(pq *parquetReader) (err error) {
			switch format {
			case "message":
				_, err = fmt.Fprintln(e.Stdout, pq.Schema())
			case "physical", "p":
				_, err = fmt.Fprintln(e.Stdout, pq.Schema().GoType())
			case "logical", "l":
				_, err = fmt.Fprintln(e.Stdout, goLogicalType(pq.Schema()))
			}
			return err
		})
	})
}

func printFile(ctx context.Context, e call.Environ, format string, head, tail int64, filter, shape string, files []string) error {
	return eachFile(files, func(name string) error {
		return withReader(name, func(pq *parquetReader) error {
			return withWriter(format, e.Stdout, func(write WriteFunc) error {
				return eachRow(pq, head, tail, makeFilter(filter, (reshape(shape, pq, write))))
			})
		})
	})
}

type WriteCloser interface {
	Write(v reflect.Value) error
	Close() error
}

const filterHelp = `
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

const shapeHelp = `
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

func makeFilter(filter string, w WriteFunc) WriteFunc {
	if filter == "" {
		return w
	}
	f, err := ParseReflectFilter(string(filter))
	if err != nil {
		return func(reflect.Value) error { return err }
	}
	return func(v reflect.Value) error {
		if include, err := f.Eval(v); err != nil {
			return err
		} else if include {
			return w(v)
		}
		return nil
	}
}

func eachFile(files []string, do func(name string) error) error {
	for _, name := range files {
		if err := do(name); err != nil {
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

func eachRow(pq *parquetReader, head, tail int64, do WriteFunc) error {
	rows := pq.NumRows()
	var start, stop int64 = 0, rows

	switch {
	case head != 0 && tail != 0:
		return fmt.Errorf("only one of --head and --tail may be provided")
	case head > 0:
		stop = head
	case head < 0:
		start = -head
	case tail > 0:
		start = rows - tail
	case tail < 0:
		stop = rows + tail
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
