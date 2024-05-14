package run

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"unsafe"
)

type Option interface {
	description() string
	seeAlso() []*Command
	setSeeAlso(cmds ...*Command)
	parseDefault(string) error
	okValues() []string
	okPrefix() string
}

type optionFlagArg interface {
	parseArg(FlagArgSource) error
}

type optionSlice interface {
	parseMany(ArgSource) error
}

type option[T any] struct {
	name     string
	desc     string
	value    *T
	parse    func(string) error
	prefixOK string   // set to - to allow -[^-]+, or -- to allow --.+ in arg context
	strOK    []string // include unusual values such as - to allow them in arg context
	see      []*Command
}

func (o *option[T]) description() string               { return o.desc }
func (o *option[T]) seeAlso() []*Command               { return o.see }
func (o *option[T]) setSeeAlso(cmds ...*Command)       { o.see = cmds }
func (o *option[T]) parseDefault(arg string) error     { return o.parse(arg) }
func (o *option[T]) parseArg(args FlagArgSource) error { return argParser(o.parse)(args) }
func (o *option[T]) okValues() []string                { return o.strOK }
func (o *option[T]) okPrefix() string                  { return o.prefixOK }

func (o *option[T]) Value() T { return *o.value }

// Flags returns a flag definition for this option with custom aliases.
// Zero values will omit either short or long. Do not omit both.
func (o *option[T]) Flags(short rune, long string, placeholder string) Flag {
	return Flag{option: o, rune: short, string: long, hint: placeholder}
}

// Flag returns a flag definition for this option using its name as the long.
// Thus an option named "opt" will have a flag name "--opt".
func (o *option[T]) Flag() Flag {
	return Flag{option: o, string: o.name}
}

// Pos returns an Arg definition for this option with a custom alias.
func (o *option[T]) Pos(name string) Arg {
	return Arg{option: o, name: name}
}

// Slice returns a Param that converts a T to a []T
func (o *option[T]) Slice() Param[[]T] {
	return sliceOf[T]{o.value}
}

func (o *option[T]) SeeAlso(cmd *Command) {
	o.see = append(o.see, cmd)
}

type FlagArgSource interface {
	Peek() (string, bool)
	Next() (string, bool)
}

type ArgSource interface {
	PeekMany() (string, bool)
	Next() (string, bool)
}

func String(name, desc string) option[string] {
	return StringLike[string](name, desc)
}

func StringLike[T ~string](name, desc string) option[T] {
	var v T
	parse := func(s string) error { v = T(s); return nil }
	return option[T]{
		name:  name,
		desc:  desc,
		value: &v,
		parse: parse,
	}
}

type NamedValue[T any] struct {
	Name  string
	Desc  string
	Value T
}

// OneStringOf defines an option whose value must be one of the provided names.
// This is suitable for small to medium sets of string-like names.
func OneStringOf[T ~string](name, desc string, names ...T) option[T] {
	nvs := make([]NamedValue[T], len(names))
	for i, nam := range names {
		nvs[i] = NamedValue[T]{Name: string(nam), Value: nam}
	}
	return OneNameOf(name, desc, nvs)
}

// OneNameOf defines an option whose value must be one of the provided names.
// This is suitable for small to medium sets of names.
func OneNameOf[T any](name, desc string, names []NamedValue[T]) option[T] {
	var v T
	names = slices.Clone(names)
	parse := func(s string) error {
		pos := slices.IndexFunc(names, func(v NamedValue[T]) bool {
			return v.Name == s
		})
		if pos < 0 {
			return NotOneOfError[T]{s, names}
		}
		v = T(names[pos].Value)
		return nil
	}
	return option[T]{
		name:  name,
		desc:  desc,
		value: &v,
		parse: parse,
	}
}

func File(name, desc string) option[string] {
	return FileLike[string](name, desc)
}

func FileLike[T ~string](name, desc string) option[T] {
	var v T
	parse := func(s string) error { v = T(s); return nil }
	return option[T]{
		name:  name,
		desc:  desc,
		value: &v,
		parse: parse,
		strOK: []string{"-"},
	}
}

func Int(name, desc string) option[int] {
	return IntLike[int](name, desc)
}

func IntLike[T ~int | ~int8 | ~int16 | ~int32 | ~int64](name, desc string) option[T] {
	var v T
	parse := func(s string) error {
		i, err := strconv.ParseInt(s, 10, int(unsafe.Sizeof(v))*8)
		if e, ok := err.(*strconv.NumError); ok || errors.As(err, &e) {
			return fmt.Errorf("parsing %q as %T: %v", e.Num, v, e.Err)
		}
		if err != nil {
			return err
		}
		v = T(i)
		return nil
	}
	return option[T]{
		name:     name,
		desc:     desc,
		value:    &v,
		parse:    parse,
		prefixOK: "-",
	}
}

func Uint(name, desc string) option[uint] {
	return UintLike[uint](name, desc)
}

func UintLike[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](name, desc string) option[T] {
	var v T
	parse := func(s string) error {
		i, err := strconv.ParseUint(s, 10, int(unsafe.Sizeof(v))*8)
		if e, ok := err.(*strconv.NumError); ok || errors.As(err, &e) {
			return fmt.Errorf("parsing %q as %T: %v", e.Num, v, e.Err)
		}
		if err != nil {
			return err
		}
		v = T(i)
		return nil
	}
	return option[T]{
		name:  name,
		desc:  desc,
		value: &v,
		parse: parse,
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

type sliceOf[T any] struct{ value *T }

func (s sliceOf[T]) Value() []T { return []T{*s.value} }
