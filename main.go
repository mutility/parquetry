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

	"github.com/mutility/parquetry/run"
)

type parquetReader = parquet.Reader //nolint:staticcheck

func main() {
	err := Run()
	if err != nil {
		os.Exit(1)
	}
}

type (
	SchemaFormat string
	DataFormat   string
	Filter       string
	Shape        string
)

func Run() error {
	schemaFormats := []run.NamedValue[SchemaFormat]{
		{Name: "message", Value: "message"},
		{Name: "m", Value: "message"},
		{Name: "logical", Value: "logical"},
		{Name: "l", Value: "logical"},
		{Name: "physical", Value: "physical"},
		{Name: "p", Value: "physical"},
	}
	meta := run.OneNameOf("format", "Output schema as message or logical/physical struct", schemaFormats)
	data := run.OneStringOf[DataFormat]("format", "Output as go, csv, json, or jsonl", "go", "csv", "json", "jsonl")
	head := run.IntLike[int64]("head", "Include first n or skip first -n rows")
	tail := run.IntLike[int64]("tail", "Include last n or skip last -n rows")
	filt := run.StringLike[Filter]("filter", "Include rows matching FILTER")
	shap := run.StringLike[Shape]("shape", "Transform rows into SHAPE")

	file := run.File("file", "Parquet file")
	files := run.FileSlice("file", "Parquet files")

	headFlag := head.Flags(0, "head", "n|-n")
	tailFlag := tail.Flags(0, "tail", "n|-n")
	dataFlag := data.Flags('f', "format", "").Default("go")

	printOne := run.Handler6(printFile, &data, &head, &tail, &filt, &shap, file.Slice())
	printMany := run.Handler6(printFile, &data, &head, &tail, &filt, &shap, &files)

	catCmd, _ := run.CmdOpt("cat", "Print a parquet file",
		run.Flags(dataFlag, headFlag, tailFlag),
		run.Args(files.Rest("file")),
		printMany,
	)

	headCmd, _ := run.CmdOpt("head", "Print (or skip) the beginning of a parquet file",
		run.Flags(dataFlag),
		run.Args(head.Pos("rows"), file.Pos("file")),
		printOne,
	)

	tailCmd, _ := run.CmdOpt("tail", "Print (or skip) the ending of a parquet file",
		run.Flags(dataFlag),
		run.Args(tail.Pos("rows"), file.Pos("file")),
		printOne,
	)

	schemaCmd, _ := run.CmdOpt("schema", "Print a parquet schema",
		run.Flags(meta.Flags('f', "format", "").Default("message")),
		run.Args(files.Rest("file")),
		run.Handler2(printSchema, &meta, &files),
	)

	toCmd, _ := run.CmdOpt("to", "Convert parquet to...",
		run.Flags(headFlag, tailFlag),
		run.Args(data.Pos("format"), files.Rest("file")),
		printMany,
	)

	whereCmd, _ := run.CmdOpt("where", "Filter a parquet file",
		run.Flags(dataFlag, shap.Flags('x', "shape", "SHAPE")),
		run.Args(filt.Pos("filter"), files.Rest("file")),
		run.DetailsFor(filterHelp, &filt),
		printMany,
	)

	reshapeCmd, _ := run.CmdOpt("reshape", "Reshape a parquet file",
		run.Flags(dataFlag, filt.Flags('m', "filter", "FILTER")),
		run.Args(shap.Pos("shape"), files.Rest("file")),
		run.DetailsFor(shapeHelp, &shap),
		printMany,
	)

	app, _ := run.AppOpt("parquetry", "Tooling for parquet files",
		run.Commands(catCmd, headCmd, tailCmd, schemaCmd, toCmd, whereCmd, reshapeCmd),
	)

	err := app.MainEnv(context.Background(), run.DefaultEnviron())
	if err != nil {
		app.Ferror(os.Stderr, err)
	}
	return err
}

func printSchema(ctx context.Context, env run.Environ, format SchemaFormat, files []string) error {
	return eachFile(files, func(name string) error {
		return withReader(name, func(pq *parquetReader) (err error) {
			switch format {
			case "message":
				_, err = fmt.Fprintln(env.Stdout, pq.Schema())
			case "physical":
				_, err = fmt.Fprintln(env.Stdout, pq.Schema().GoType())
			case "logical":
				s := goLogicalType(pq.Schema(), false).String()
				_, err = fmt.Fprintln(env.Stdout, strings.ReplaceAll(s, " main.", " "))
			}
			return err
		})
	})
}

func printFile(ctx context.Context, env run.Environ, format DataFormat, head, tail int64, expr Filter, shape Shape, files []string) error {
	return eachFile(files, func(name string) error {
		return withReader(name, func(pq *parquetReader) error {
			return withWriter(format, env.Stdout, func(write WriteFunc) error {
				write, err := reshapeWrite(shape, pq, write)
				if err != nil {
					return err
				}
				write, err = filterWrite(expr, pq, write)
				if err != nil {
					return err
				}
				return eachRow(pq, head, tail, write)
			})
		})
	})
}

type WriteCloser interface {
	Write(v reflect.Value) error
	Close() error
}

const filterHelp = `
Specify the desired filter per the expr language, a go-like syntax.

Full details at https://expr-lang.org/docs/language-definition

Compare a dotted name to literals true/false/null, integers, dates, times, or strings.
Values must generally match the type of the data to which they are compared.
Dates, times, and timestamps can also be compared to properly formatted strings, integers (per their physical storage), and either date(...) or duration(...) values.

Comparisons include == (equality), != (inequality), < (less), > (greater),
<= (less or equal), >= (greater or equal), in (membership)
Group comparisons with optional (), and tweak with a leading ! (not), adjoining && (and) or || (or).

Examples:
  - someBool == true || someBool == false
  - some.nested.boolean || !some.other.boolean
  - somePointer != nil
  - someString < "b"
  - timestamp < "2024-01-01T01:01:01.111Z"
  - time > "01:01:01.111Z"
  - day >= "2024-01-01" && date < "2025-01-01"
  - indexOf(a, "e")!=-1 && indexOf(a, "f")==-1
  - someInt in [1, 2, 3]
  - !(someString in ["d", "e", "f"])
  - !((anInt == 1 && otherInt == 2) || (anInt == 2 && otherInt == 1))
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

func withWriter(format DataFormat, w io.Writer, do func(WriteFunc) error) error {
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
	return fmt.Errorf("format %q: %w", format, errors.ErrUnsupported)
}

type WriteFunc func(reflect.Value) error

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

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}

	pq := parquet.NewReader(pf)

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
	rowType := goLogicalType(pq.Schema(), true)
	v, z := reflect.New(rowType), reflect.Zero(rowType)

	if start > 0 {
		if err := pq.SeekToRow(start); err != nil {
			return err
		}
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
func goLogicalType(s *parquet.Schema, tag bool) reflect.Type {
	return reflect.StructOf(goLogicalTypeFields(s.Fields(), nil, tag))
}

func goLogicalTypeFields(flds []parquet.Field, path []string, tag bool) []reflect.StructField {
	sf := make([]reflect.StructField, len(flds))
	for i, pf := range flds {
		sf[i] = goLogicalTypeField(pf, append(path, pf.Name()), tag)
	}
	return sf
}

func goLogicalTypeField(pf parquet.Field, path []string, tag bool) reflect.StructField {
	name := pf.Name()
	title := strings.ToTitle(name[:1]) + name[1:]
	sf := reflect.StructField{
		Name: title,
		Type: pf.GoType(),
	}
	if name != title && tag {
		sf.Tag = reflect.StructTag(fmt.Sprintf("json:%[1]q parquet:%[1]q expr:%[1]q", name))
	}

	if lt := pf.Type().LogicalType(); lt != nil {
		switch {
		case lt.UTF8 != nil:
			sf.Type = reflect.TypeFor[string]()
		case lt.Map != nil:
			kvs := pf.Fields()[0]
			mapfields := goLogicalTypeFields(kvs.Fields(), append(path, kvs.Name()), tag)
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
	} else if !pf.Leaf() {
		sf.Type = reflect.StructOf(goLogicalTypeFields(pf.Fields(), path, tag))
	}
	if k := sf.Type.Kind(); pf.Optional() && k != reflect.Pointer && k != reflect.Map {
		sf.Type = reflect.PointerTo(sf.Type)
	}
	return sf
}
