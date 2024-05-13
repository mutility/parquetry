package call

import (
	"fmt"
	"slices"
	"strconv"
)

type Flag interface {
	ID() (string, string)
	DefaultString() string
	Placeholder() string
	See() []*command

	dashes() Dash
	setDashes(Dash)
	assigned() bool
	reset()
	seeAlso(*command)
}

type flagDef[T, P any] struct {
	p             *T
	set           bool
	name          string
	description   string
	defaultValue  T
	defaultString string
	hasDefault    bool
	placeholder   string
	dashable      Dash
	parser        parser[P]
	seeCmds       []*command // commands with related help detail
}

func (f *flagDef[T, P]) String() string {
	return fmt.Sprintf("%s=%v", f.name, *f.p)
}

func (f *flagDef[T, P]) ID() (string, string) {
	return f.name, f.description
}

func (f *flagDef[T, P]) DefaultString() string { return f.defaultString }
func (f *flagDef[T, P]) Placeholder() string   { return f.placeholder }
func (f *flagDef[T, P]) DashNumber()           { f.dashable |= DashNumber }
func (f *flagDef[T, P]) dashes() Dash          { return f.dashable }
func (f *flagDef[T, P]) setDashes(d Dash)      { f.dashable = d }
func (f *flagDef[T, P]) setParser(p parser[P]) { f.parser = p }
func (f *flagDef[T, P]) assigned() bool        { return f.set }
func (f *flagDef[T, P]) reset()                { f.set = false }
func (f *flagDef[T, P]) See() []*command       { return slices.Clone(f.seeCmds) }
func (f *flagDef[T, P]) seeAlso(c *command)    { f.seeCmds = append(f.seeCmds, c) }
func (f *flagDef[T, P]) setDefault(val T, repr string) {
	*f.p = val
	f.defaultValue, f.defaultString, f.hasDefault = val, repr, true
	if repr == "" {
		f.defaultString = fmt.Sprintf(`"%v"`, val)
	}
}

var ( // suppress staticcheck false positives...
	_ = (*flagDef[int, int]).setParser
	_ = (*flagDef[int, int]).setDefault
)

func (f *flagDef[T, P]) Value() T {
	if !f.set && f.hasDefault {
		return f.defaultValue
	}
	return *f.p
}

func noParse[T any](string) (zero T, err error) { return zero, UnexpectedArgError{} }

func validate[T any](valid func(T) error, value T) error {
	if valid == nil {
		return nil
	}
	return valid(value)
}

func parserFor[T any]() func(string) (T, error) {
	var zero T
	parse, _ := map[any]any{
		int(0):     parseInt[int],
		int8(0):    parseInt[int8],
		int16(0):   parseInt[int16],
		int32(0):   parseInt[int32],
		int64(0):   parseInt[int64],
		uint(0):    parseUint[uint],
		uint8(0):   parseUint[uint8],
		uint16(0):  parseUint[uint16],
		uint32(0):  parseUint[uint32],
		uint64(0):  parseUint[uint64],
		float32(0): parseFloat[float32],
		float64(0): parseFloat[float64],
		false:      parseBool[bool],
		"":         parseString[string],
	}[zero].(func(string) (T, error))
	return parse
}

func bitsFor[T any]() int {
	var zero T
	return map[any]int{
		int(0):     strconv.IntSize,
		int8(0):    8,
		int16(0):   16,
		int32(0):   32,
		int64(0):   64,
		uint(0):    strconv.IntSize,
		uint8(0):   8,
		uint16(0):  16,
		uint32(0):  32,
		uint64(0):  64,
		float32(0): 32,
		float64(0): 64,
	}[zero]
}

func parseBool[T ~bool](s string) (T, error) {
	v, err := strconv.ParseBool(s)
	return T(v), err
}

func parseInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](s string) (T, error) {
	v, err := strconv.ParseInt(s, 10, bitsFor[T]())
	return T(v), err
}

func parseUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](s string) (T, error) {
	v, err := strconv.ParseUint(s, 10, bitsFor[T]())
	return T(v), err
}

func parseFloat[T ~float32 | ~float64](s string) (T, error) {
	v, err := strconv.ParseFloat(s, bitsFor[T]())
	return T(v), err
}

func parseString[T ~string](s string) (T, error) {
	return T(s), nil
}

func flagsOn[T flagConsumer](f T, flags string, cmds []Command) T {
	for _, cmd := range cmds {
		cmd.addFlag(flags, f)
	}
	return f
}

func posOn[T posConsumer](f T, name string, cmds []Command) T {
	if name == "" {
		name, _ = f.ID()
	}
	for _, cmd := range cmds {
		cmd.addPos(f, name)
	}
	return f
}

func isNumeric(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}
