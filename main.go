package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
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

var headFlag, tailFlag int64

func main() {
	if err := run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	headtailFlags := []cli.Flag{
		&cli.Int64Flag{
			Name:   "head",
			Action: func(_ *cli.Context, i int64) error { headFlag = i; return nil },
			Usage:  "output only first N rows; skip if negative",
		},
		&cli.Int64Flag{
			Name:   "tail",
			Action: func(_ *cli.Context, i int64) error { tailFlag = i; return nil },
			Usage:  "output only last N rows; skip if negative",
		},
	}

	app := cli.App{
		Name:  "parquetry",
		Usage: "Tooling for parquet files",
		Commands: []*cli.Command{
			{
				Name:   "cat",
				Flags:  headtailFlags,
				Action: eachFile(catFile),
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
					{Name: "csv", Flags: headtailFlags, Action: eachFile(printCSV)},
					{Name: "json", Flags: headtailFlags, Action: eachFile(printJSON)},
					{Name: "jsonl", Flags: headtailFlags, Action: eachFile(printJSONL)},
				},
			},
			{
				Name:   "where",
				Action: thenEachFile(ParseReflectFilter, printWhere),
			},
		},
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	err := app.RunContext(ctx, args)
	cancel()
	return err
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

func withReader(name string, do func(*parquet.Reader) error) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	pq := parquet.NewReader(f)
	defer pq.Close()

	return do(pq)
}

func headtail(c *cli.Context, pq *parquet.Reader) (rng struct{ Start, Stop int64 }, err error) {
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

func eachRow(c *cli.Context, pq *parquet.Reader, do func(reflect.Value) error) error {
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

func catFile(c *cli.Context, name string) error {
	w := c.App.Writer
	return withReader(name, func(pq *parquet.Reader) error {
		return eachRow(c, pq, func(v reflect.Value) error {
			_, err := fmt.Fprintf(w, "%+v\n", v.Interface())
			return err
		})
	})
}

func printSchema(c *cli.Context, name string) error {
	return withReader(name, func(pq *parquet.Reader) (err error) {
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

func printCSV(c *cli.Context, name string) error {
	return withReader(name, func(pq *parquet.Reader) error {
		w := csv.NewWriter(c.App.Writer)
		fields := pq.Schema().Fields()
		hdr := make([]string, len(fields))
		for i, f := range fields {
			hdr[i] = f.Name()
		}
		if err := w.Write(hdr); err != nil {
			return err
		}

		vals := make([]string, len(fields))
		eachRow(c, pq, func(v reflect.Value) error {
			for i := range fields {
				switch v := v.Field(i).Interface().(type) {
				case string, int, int64, float32, float64,
					LocDate, UTCDate,
					TimeMilliLoc, TimeMilliUTC, TimeMicroLoc, TimeMicroUTC, TimeNanoLoc, TimeNanoUTC,
					StampMilliLoc, StampMilliUTC, StampMicroLoc, StampMicroUTC, StampNanoLoc, StampNanoUTC:
					vals[i] = fmt.Sprint(v)
				default:
					b, err := json.Marshal(v)
					if err != nil {
						return err
					}
					vals[i] = string(b)
				}
			}
			return w.Write(vals)
		})

		w.Flush()
		return w.Error()
	})
}

func printJSON(c *cli.Context, name string) error {
	w := c.App.Writer
	b := &bytes.Buffer{}
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "")
	prefix, suffix := "[\n  %s", "[]\n"
	if err := withReader(name, func(pq *parquet.Reader) error {
		return eachRow(c, pq, func(v reflect.Value) error {
			if err := enc.Encode(v.Interface()); err != nil {
				return err
			}
			j := bytes.TrimSuffix(b.Bytes(), []byte{'\n'})

			_, err := fmt.Fprintf(w, prefix, j)
			prefix = ",\n  %s"
			suffix = "\n]\n"
			b.Reset()
			return err
		})
	}); err != nil {
		return err
	}
	_, err := io.WriteString(w, suffix)
	return err
}

func printJSONL(c *cli.Context, name string) error {
	enc := json.NewEncoder(c.App.Writer)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "")
	return withReader(name, func(pq *parquet.Reader) error {
		return eachRow(c, pq, func(v reflect.Value) error {
			return enc.Encode(v.Interface())
		})
	})
}

func printWhere(c *cli.Context, where ReflectFilter, name string) error {
	w := c.App.Writer
	return withReader(name, func(pq *parquet.Reader) error {
		return eachRow(c, pq, func(v reflect.Value) error {
			if where(v) {
				_, err := fmt.Fprintf(w, "%+v\n", v.Interface())
				return err
			}
			return nil
		})
	})
}
