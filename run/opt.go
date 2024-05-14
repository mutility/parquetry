package run

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"unsafe"
)

type Option struct {
	name string
	desc string
	storage
	parseDefault func(string) error
	parseArg     func(FlagArgSource) error
	parseMany    func(ArgSource) error
	prefixOK     string   // set to - to allow -[^-]+, or -- to allow --.+ in arg context
	strOK        []string // include unusual values such as - to allow them in arg context
	seeAlso      []*Command
}

// Flags returns a flag definition for this option with custom aliases.
// Zero values will omit either short or long. Do not omit both.
func (o *Option) Flags(short rune, long string, placeholder string) Flag {
	return Flag{option: o, rune: short, string: long, hint: placeholder}
}

// Flag returns a flag definition for this option using its name as the long.
// Thus an option named "opt" will have a flag name "--opt".
func (o *Option) Flag() Flag {
	return Flag{option: o, string: o.name}
}

// Pos returns an Arg definition for this option with a custom alias.
func (o *Option) Pos(name string) Arg {
	return Arg{option: o, name: name}
}

// Rest returns an multi-Arg definition for this option with a custom alias.
func (o *Option) Rest(name string) Arg {
	return Arg{option: o, name: name, many: true}
}

// Slice returns a Param that converts a T to a []T
func (o *Option) Slice() *Option {
	type asSlicer interface {
		asSlice() storage
	}
	return &Option{storage: o.storage.(asSlicer).asSlice()}
}

func (o *Option) SeeAlso(cmd *Command) {
	o.seeAlso = append(o.seeAlso, cmd)
}

type FlagArgSource interface {
	Peek() (string, bool)
	Next() (string, bool)
}

type ArgSource interface {
	PeekMany() (string, bool)
	Next() (string, bool)
}

func String(name, desc string) Option {
	return StringLike[string](name, desc)
}

func StringLike[T ~string](name, desc string) Option {
	var v store[T]
	parse := func(s string) error { v.Set(T(s)); return nil }
	return Option{
		name:         name,
		desc:         desc,
		storage:      &v,
		parseDefault: parse,
		parseArg:     argParser(parse),
	}
}

type NamedValue[T any] struct {
	Name  string
	Desc  string
	Value T
}

// OneStringOf defines an option whose value must be one of the provided names.
// This is suitable for small to medium sets of string-like names.
func OneStringOf[T ~string](name, desc string, names ...T) Option {
	nvs := make([]NamedValue[T], len(names))
	for i, nam := range names {
		nvs[i] = NamedValue[T]{Name: string(nam), Value: nam}
	}
	return OneNameOf(name, desc, nvs)
}

// OneNameOf defines an option whose value must be one of the provided names.
// This is suitable for small to medium sets of names.
func OneNameOf[T any](name, desc string, names []NamedValue[T]) Option {
	var v store[T]
	names = slices.Clone(names)
	parse := func(s string) error {
		pos := slices.IndexFunc(names, func(v NamedValue[T]) bool {
			return v.Name == s
		})
		if pos < 0 {
			return NotOneOfError[T]{s, names}
		}
		v.Set(T(names[pos].Value))
		return nil
	}
	return Option{
		name:         name,
		desc:         desc,
		storage:      &v,
		parseDefault: parse,
		parseArg:     argParser(parse),
	}
}

func File(name, desc string) Option {
	return FileLike[string](name, desc)
}

func FileLike[T ~string](name, desc string) Option {
	var v store[T]
	parse := func(s string) error { v.Set(T(s)); return nil }
	return Option{
		name:         name,
		desc:         desc,
		storage:      &v,
		parseDefault: parse,
		parseArg:     argParser(parse),
		strOK:        []string{"-"},
	}
}

func Int(name, desc string) Option {
	return IntLike[int](name, desc)
}

func IntLike[T ~int | ~int8 | ~int16 | ~int32 | ~int64](name, desc string) Option {
	var v store[T]
	parse := func(s string) error {
		i, err := strconv.ParseInt(s, 10, int(unsafe.Sizeof(v.value))*8)
		if e, ok := err.(*strconv.NumError); ok || errors.As(err, &e) {
			return fmt.Errorf("parsing %q as %T: %v", e.Num, v.value, e.Err)
		}
		if err != nil {
			return err
		}
		v.Set(T(i))
		return nil
	}
	return Option{
		name:         name,
		desc:         desc,
		storage:      &v,
		parseDefault: parse,
		parseArg:     argParser(parse),
		prefixOK:     "-",
	}
}

func Uint(name, desc string) Option {
	return UintLike[uint](name, desc)
}

func UintLike[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](name, desc string) Option {
	var v store[T]
	parse := func(s string) error {
		i, err := strconv.ParseUint(s, 10, int(unsafe.Sizeof(v.value))*8)
		if e, ok := err.(*strconv.NumError); ok || errors.As(err, &e) {
			return fmt.Errorf("parsing %q as %T: %v", e.Num, v.value, e.Err)
		}
		if err != nil {
			return err
		}
		v.Set(T(i))
		return nil
	}
	return Option{
		name:         name,
		desc:         desc,
		storage:      &v,
		parseDefault: parse,
		parseArg:     argParser(parse),
	}
}

var errMissingArg = errors.New("no argument provided")

func argParser(parse func(string) error) func(FlagArgSource) error {
	return func(s FlagArgSource) error {
		v, ok := s.Peek()
		if !ok {
			return errMissingArg
		}
		err := parse(v)
		if err == nil {
			s.Next()
		}
		return err
	}
}

type storage interface {
	Any() any
}

type (
	store[T any]       struct{ value T }
	storeSingle[T any] store[T]
)

func (s *store[T]) Any() any         { return s.value }
func (s *store[T]) Value() T         { return s.value }
func (s *store[T]) Set(v T)          { s.value = v }
func (s *store[T]) asSlice() storage { return (*storeSingle[T])(s) }

func (s *storeSingle[T]) Any() any   { return []T{s.value} }
func (s *storeSingle[T]) Value() []T { return []T{s.value} }
