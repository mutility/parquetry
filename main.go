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

	"github.com/dustin/go-humanize"
	"github.com/mutility/cli/run"
	"github.com/parquet-go/parquet-go"
)

type parquetReader = parquet.Reader //nolint:staticcheck

func main() {
	os.Exit(run.Main(runEnv))
}

type (
	SchemaFormat string
	DataFormat   string
	Filter       string
	Shape        string
)

func runEnv(env run.Environ) error {
	schemaFormats := []run.NamedValue[SchemaFormat]{
		{Name: "message", Value: "message"},
		{Name: "m", Value: "message"},
		{Name: "logical", Value: "logical"},
		{Name: "l", Value: "logical"},
		{Name: "physical", Value: "physical"},
		{Name: "p", Value: "physical"},
	}

	typer := new(schemata)
	stringify := run.EnablerVar(&typer.Stringify, "string", "Treat all []uint as string.", true)

	schemaFmt := run.NamedOf("format", "Output schema as message or logical/physical struct", schemaFormats)
	outFmt := run.StringOf[DataFormat]("format", "Output as go, csv, json, or jsonl", "go", "csv", "json", "jsonl")
	head := run.IntLike[int64]("head", "Include first n or skip first -n rows", 0)
	tail := run.IntLike[int64]("tail", "Include last n or skip last -n rows", 0)
	filter := run.StringLike[Filter]("filter", "Include rows matching FILTER")
	shape := run.StringLike[Shape]("shape", "Transform rows into SHAPE")

	file := run.File("file", "Parquet file")
	files := run.FileSlice("file", "Parquet files")

	headFlag := head.Flags(0, "head", "n|-n")
	tailFlag := tail.Flags(0, "tail", "n|-n")
	dataFlag := outFmt.Flags('f', "format", "").Default("go")

	printOne := run.Handler7(printFile, outFmt, head, tail, filter, shape, file.Slice(), run.Pass(typer))
	printMany := run.Handler7(printFile, outFmt, head, tail, filter, shape, files, run.Pass(typer))

	app := run.MustApp("parquetry", "Tooling for parquet files",
		stringify.Flag(),
		run.MustCmd("cat", "Print a parquet file",
			dataFlag, headFlag, tailFlag,
			files.Args("file"),
			printMany,
		),

		run.MustCmd("head", "Print (or skip) the beginning of a parquet file",
			dataFlag,
			head.Arg("rows"), file.Arg("file"),
			printOne,
		),

		run.MustCmd("tail", "Print (or skip) the ending of a parquet file",
			dataFlag,
			tail.Arg("rows"), file.Arg("file"),
			printOne,
		),

		run.MustCmd("meta", "Print parquet metadata",
			files.Args("file"),
			run.Handler2(printMeta, files, run.Pass(typer)),
		),

		run.MustCmd("schema", "Print parquet schema",
			schemaFmt.Flags('f', "format", "").Default("message"),
			files.Args("file"),
			run.Handler3(printSchema, schemaFmt, files, run.Pass(typer)),
		),

		run.MustCmd("to", "Convert parquet to...",
			headFlag, tailFlag,
			outFmt.Arg("format"), files.Args("file"),
			printMany,
		),

		run.MustCmd("where", "Filter a parquet file",
			dataFlag, shape.Flags('x', "shape", "SHAPE"),
			filter.Arg("filter"), files.Args("file"),
			run.DetailsFor(filterHelp, filter),
			printMany,
		),

		run.MustCmd("reshape", "Reshape a parquet file",
			dataFlag, filter.Flags('m', "filter", "FILTER"),
			shape.Arg("shape"), files.Args("file"),
			run.DetailsFor(shapeHelp, shape),
			printMany,
		),
	)

	err := app.Main(context.Background(), run.DefaultEnviron())
	if err != nil {
		app.Ferror(env.Stderr, err)
	}
	return err
}

func printMeta(ctx run.Context, files []string, typer *schemata) error {
	return eachFile(files, func(name string) error {
		return withFile(name, func(pf *parquet.File) (err error) {
			m := pf.Metadata()
			if len(files) > 1 {
				fmt.Fprintln(ctx.Stdout, name+":")
			}
			fmt.Fprintln(ctx.Stdout, "created by:", m.CreatedBy)
			fmt.Fprintln(ctx.Stdout, "format:", m.Version)
			fmt.Fprintln(ctx.Stdout, "columns:", m.Schema[0].NumChildren)
			fmt.Fprintln(ctx.Stdout, "rows:", m.NumRows)
			fmt.Fprintln(ctx.Stdout, "row groups:", len(m.RowGroups))
			for _, rg := range m.RowGroups {
				if rg.TotalByteSize != rg.TotalCompressedSize {
					fmt.Fprintf(ctx.Stdout, "  %d: %s (%s in file) at offset %x\n", rg.Ordinal, humanize.IBytes(uint64(rg.TotalByteSize)), humanize.IBytes(uint64(rg.TotalCompressedSize)), rg.FileOffset)
				} else {
					fmt.Fprintf(ctx.Stdout, "  %d: %s at offset %x\n", rg.Ordinal, humanize.IBytes(uint64(rg.TotalByteSize)), rg.FileOffset)
				}
			}
			for _, kvm := range m.KeyValueMetadata {
				fmt.Fprintln(ctx.Stdout, "meta:", kvm.Key, "=", kvm.Value)
			}

			return nil
		})
	})
}

func printSchema(ctx run.Context, format SchemaFormat, files []string, typer *schemata) error {
	return eachFile(files, func(name string) error {
		return withReader(name, func(pq *parquetReader) (err error) {
			var schema any
			switch format {
			case "message":
				schema = pq.Schema()
			case "physical":
				schema = pq.Schema().GoType()
			case "logical":
				s := typer.Logical(pq.Schema()).String()
				schema = strings.ReplaceAll(s, " main.", " ")
			}
			if len(files) > 1 {
				_, err = fmt.Fprintln(ctx.Stdout, name+":", schema)
			} else {
				_, err = fmt.Fprintln(ctx.Stdout, schema)
			}
			return err
		})
	})
}

func printFile(ctx run.Context, format DataFormat, head, tail int64, expr Filter, shape Shape, files []string, typer *schemata) error {
	return eachFile(files, func(name string) error {
		return withReader(name, func(pq *parquetReader) error {
			return withWriter(format, ctx.Stdout, func(write WriteFunc) error {
				rowType := typer.LogicalTagged(pq.Schema())
				write, err := reshapeWrite(shape, rowType, write)
				if err != nil {
					return err
				}
				write, err = filterWrite(expr, rowType, write)
				if err != nil {
					return err
				}
				return eachRow(pq, head, tail, rowType, write)
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
Records for which the expression evaluates to true will be included in the output.

  - Comparisons include:  ==  !=  <  <=  >  >=  in  contains  matches
  - Logical algebra includes:  !  not  &&  and  ||  or
  - Precedence can be overridden with:  (…)
  - Values include:  true  false  nil  42  1.4  "hi"  [1, 2]  {a: 1, b: 2}
  - Fields and nested fields are referenced by name:  a  b.c

Expressions are evaluated in the context of each row of the parquet file.
Each logical field is available using its name from the schema with the type in the logical schema.
The names are case sensitive and remain lowercase even when the logical schema has capitalized them.
Logical dates, times, and timestamps can be compared to others of the same type, to integers matching their physical storage, or to strings representing their value.
Times can be represented duration strings (10h3m2.1s).

Reference https://expr-lang.org/docs/language-definition for full details.

Given a parquet file with lowercase names and logical schema:
	struct {
		F bool; Pf *bool
		I, J, K int32
		M map[string]string
		Ps, Rs string
		W struct { D Date; T TimeMilliUTC; S StampMilliUTC }
	}

Examples include:

  - true; false                 // always include/exclude
  - f; f == true; !f; not f     // conditional inclusion
  - i < j; k >= i; j != k       // comparisons in the record
  - pf != nil; pf ?? true       // nil handling, coalescing
  - rs < "b"; rs contains "y"   // string comparisons
  - i in [1,2]; rs in ["a","b"] // membership checks
  - not(i < j and (rs contains "q" || rs == "u"))
  - w.d == "2024-01-01"; w.d > 7300                   // (days since epoch)
  - w.t == "14:22:59"; w.t > "13h"; w.t < 1234        // (since midnight)
  - w.s < "2024-01-01T01:01:01.111Z"; w.s > 123456789 // (since epoch)
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

// once fixed...
// Dates and timestamps can also be compared to times (as returned by date(…)).
// Times can also be compaed to durations (as returned by duration(…)).

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

func withFile(name string, do func(*parquet.File) error) error {
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

	return do(pf)
}

func withReader(name string, do func(*parquetReader) error) error {
	return withFile(name, func(pf *parquet.File) error {
		pq := parquet.NewReader(pf)
		defer pq.Close()
		return do(pq)
	})
}

func eachRow(pq *parquetReader, head, tail int64, rowType reflect.Type, do WriteFunc) error {
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

type schemata struct {
	Stringify bool
	Tagged    bool
}

// Logical returns a useful go type
//
// s.GoType returns a struct corresponding to the physical structure of the parquet file.
// However logical types can be handled better. In particular...
//
// - Logical maps should use map[K]V instead of a (nested) slice of key-value structs
// - Logical strings should use string instead of []uint8
// - If Stringify is true, even non-logical string []uint8 fields become strings
func (s schemata) Logical(schema *parquet.Schema) reflect.Type {
	s.Tagged = false
	return s.logical(schema)
}

func (s schemata) LogicalTagged(schema *parquet.Schema) reflect.Type {
	s.Tagged = true
	return s.logical(schema)
}

func (s schemata) logical(schema *parquet.Schema) reflect.Type {
	return reflect.StructOf(s.logicalTypeFields(schema.Fields(), nil))
}

func (s schemata) logicalTypeFields(flds []parquet.Field, path []string) []reflect.StructField {
	sf := make([]reflect.StructField, len(flds))
	for i, pf := range flds {
		sf[i] = s.logicalTypeField(pf, append(path, pf.Name()))
	}
	return sf
}

func (s schemata) logicalTypeField(pf parquet.Field, path []string) reflect.StructField {
	name := pf.Name()
	title := strings.ToTitle(name[:1]) + name[1:]
	sf := reflect.StructField{
		Name: title,
		Type: pf.GoType(),
	}
	if name != title && s.Tagged {
		sf.Tag = reflect.StructTag(fmt.Sprintf("json:%[1]q parquet:%[1]q expr:%[1]q", name))
	}

	if lt := pf.Type().LogicalType(); lt != nil {
		switch {
		case lt.UTF8 != nil:
			sf.Type = reflect.TypeFor[string]()
		case lt.Map != nil:
			kvs := pf.Fields()[0]
			mapfields := s.logicalTypeFields(kvs.Fields(), append(path, kvs.Name()))
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
		sf.Type = reflect.StructOf(s.logicalTypeFields(pf.Fields(), path))
	} else if s.Stringify && pf.GoType() == reflect.TypeFor[[]byte]() {
		sf.Type = reflect.TypeFor[string]()
	}
	if k := sf.Type.Kind(); pf.Optional() && k != reflect.Pointer && k != reflect.Map {
		sf.Type = reflect.PointerTo(sf.Type)
	}
	return sf
}
