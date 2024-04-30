package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/urfave/cli/v2"
)

var (
	headFlag, tailFlag int64
	formatFlag         string
)

type (
	cliContext    = cli.Context
	parquetReader = parquet.Reader
)

func main() {
	if err := run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func set[T any](p *T) func(*cli.Context, T) error {
	return func(ctx *cli.Context, v T) error {
		*p = v
		return nil
	}
}

func setKnown[Discard, T any](p *T, known T) func(*cli.Context, Discard) error {
	return func(*cli.Context, Discard) error {
		*p = known
		return nil
	}
}

func run(args []string) error {
	formatFlags := []cli.Flag{
		&cli.StringFlag{
			Name:   "format",
			Value:  "go",
			Hidden: true,
		},
		&cli.BoolFlag{Name: "csv", Action: setKnown[bool](&formatFlag, "csv"), Usage: "output CSV"},
		&cli.BoolFlag{Name: "json", Action: setKnown[bool](&formatFlag, "json"), Usage: "output JSON"},
		&cli.BoolFlag{Name: "jsonl", Action: setKnown[bool](&formatFlag, "jsonl"), Usage: "output JSON-Lines"},
	}
	headtailFlags := []cli.Flag{
		&cli.Int64Flag{
			Name:   "head",
			Action: set(&headFlag),
			Usage:  "output only first N rows; skip if negative",
		},
		&cli.Int64Flag{
			Name:   "tail",
			Action: set(&tailFlag),
			Usage:  "output only last N rows; skip if negative",
		},
	}

	app := cli.App{
		Name:  "parquetry",
		Usage: "Tooling for parquet files",
		Commands: []*cli.Command{
			{
				Name:   "cat",
				Flags:  append(formatFlags, headtailFlags...),
				Action: printAs(newFormatWriter),
			},
			{
				Name: "schema",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "go", Usage: "output a go struct definition"},
					&cli.BoolFlag{Name: "physical", Usage: "output a physical go struct definition"},
					&cli.BoolFlag{Name: "logical", Usage: "output a logical go struct definition"},
				},
				Action: eachFile(printSchema),
			},
			{
				Name:  "to",
				Flags: headtailFlags,
				Subcommands: []*cli.Command{
					{Name: "csv", Flags: headtailFlags, Action: printAs(newCSVWriter)},
					{Name: "json", Flags: headtailFlags, Action: printAs(newJSONWriter)},
					{Name: "jsonl", Flags: headtailFlags, Action: printAs(newJSONLWriter)},
				},
			},
			{
				Name:   "where",
				Flags:  append(formatFlags, headtailFlags...),
				Action: thenEachFile(ParseReflectFilter, printWhere),
			},
		},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	err := app.RunContext(ctx, args)
	cancel()
	return err
}

func newFormatWriter(c *cli.Context, pq *parquetReader) RowWriteCloser {
	return map[string]func(*cli.Context, *parquetReader) RowWriteCloser{
		"":      newGoWriter,
		"go":    newGoWriter,
		"csv":   newCSVWriter,
		"json":  newJSONWriter,
		"jsonl": newJSONLWriter,
	}[c.String("format")](c, pq)
}

type RowWriteCloser interface {
	Write(v reflect.Value) error
	Close() error
}

func printAs(new func(*cli.Context, *parquetReader) RowWriteCloser) cli.ActionFunc {
	return eachFile(func(c *cli.Context, name string) error {
		return withReader(name, func(pq *parquetReader) error {
			w := new(c, pq)
			return errors.Join(eachRow(c, pq, func(v reflect.Value) error {
				return w.Write(v)
			}), w.Close())
		})
	})
}

type FilenameAction func(c *cli.Context, name string) error

func eachFile(action FilenameAction) cli.ActionFunc {
	return func(c *cli.Context) error {
		for _, name := range c.Args().Slice() {
			if err := action(c, name); err != nil {
				return err
			}
		}
		return nil
	}
}

type ThenFilenameAction[T any] func(c *cli.Context, x T, name string) error

func thenEachFile[T any](fn func(string) (T, error), action ThenFilenameAction[T]) cli.ActionFunc {
	return func(c *cli.Context) error {
		x, err := fn(c.Args().First())
		if err != nil {
			return err
		}
		for _, name := range c.Args().Tail() {
			if err := action(c, x, name); err != nil {
				return err
			}
		}
		return nil
	}
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

func headtail(c *cli.Context, pq *parquetReader) (rng struct{ Start, Stop int64 }, err error) {
	rows := pq.NumRows()
	rng.Stop = rows
	switch {
	case headFlag != 0 && tailFlag != 0:
		err = errors.New("both head and tail specified")
	case headFlag > 0:
		rng.Stop = headFlag
	case headFlag < 0:
		rng.Start = -headFlag
	case tailFlag > 0:
		rng.Start = rng.Stop - tailFlag
	case tailFlag < 0:
		rng.Stop = rng.Stop + tailFlag
	}
	if rng.Start < 0 {
		rng.Start = 0
	}
	if rng.Stop > rows {
		rng.Stop = rows
	}
	return
}

func eachRow(c *cli.Context, pq *parquetReader, do func(reflect.Value) error) error {
	rng, err := headtail(c, pq)
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
			sf.Type = reflect.TypeFor[UTCDate]()
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

func printSchema(c *cli.Context, name string) error {
	return withReader(name, func(pq *parquetReader) (err error) {
		switch {
		case c.Bool("physical"):
			_, err = fmt.Fprintln(c.App.Writer, pq.Schema().GoType())
		case c.Bool("go"):
			_, err = fmt.Fprintln(c.App.Writer, goLogicalType(pq.Schema()))
		default:
			_, err = fmt.Fprintln(c.App.Writer, pq.Schema())
		}
		return err
	})
}

func printWhere(c *cli.Context, where ReflectFilter, name string) error {
	w := c.App.Writer
	return withReader(name, func(pq *parquetReader) error {
		return eachRow(c, pq, func(v reflect.Value) error {
			if include, err := where.Eval(v); err != nil {
				return err
			} else if include {
				_, err := fmt.Fprintf(w, "%+v\n", v.Interface())
				return err
			}
			return nil
		})
	})
}
