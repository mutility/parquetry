package call

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
)

type Consumes int

const (
	ConsumesArg Consumes = iota
	ConsumesName
	ConsumesSlice
)

type Dash uint

const (
	DashNone   Dash = 0
	DashNumber Dash = 1 << iota
	DashStdio
)

var ErrMissingArgument = errors.New("missing")

type Flag interface {
	ID() (string, string)
	DefaultString() string
	Placeholder() string
	PosName() string
	See() []*command

	consumes() Consumes
	dashes() Dash
	setDashes(Dash)
	setParser(any)
	assigned() bool
	reset()
	setPosName(string)
	setSeeCmds([]*command)
}

// FlagSettings configure flags
//
//   - Parser[T](parse func(string) (T, error)): use parse to interpret arguments
//   - DashNone|DashNumber|DashStdio: allow dashes in arguments; int-style options default to DashNumber
//   - SeeCmd(cmd, ...): Refer to a command that has more detailed help for the flag
type FlagSetting interface{ configureFlag(Flag) }

func applyFlagSettings[T, P any](f flagDef[T, P], settings []FlagSetting) flagDef[T, P] {
	for _, setting := range settings {
		setting.configureFlag(&f)
	}
	if f.parser == nil {
		panic(fmt.Sprintf("%s: needs parser", f.name))
	}
	return f
}

func (d Dash) configureFlag(f Flag) {
	f.setDashes(d)
}

type parser[T any] func(string) (T, error)

func (p parser[T]) configureFlag(f Flag) {
	f.setParser(p)
}

// Parser specifies a parsing function for this flag.
// This can be used for types without default parsers, such as to accept enum names on an integer values.
func Parser[T any](p parser[T]) parser[T] { return parser[T](p) }

// SeeCmd specifies one or more commands that have detailed documentation for this flag.
func SeeCmd(cmds ...*command) seeCmds { return seeCmds(cmds) }

type seeCmds []*command

func (s seeCmds) configureFlag(f Flag) {
	f.setSeeCmds(([]*command)(s))
}

type ManyParser interface {
	Parse([]string) (int, error)
}

type SingleParser interface {
	Parse(string, string) (int, error)
}

type PresenceParser interface {
	Parse(string) (int, error)
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
	posName       string
	consumption   Consumes
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

func (f *flagDef[T, P]) DefaultString() string   { return f.defaultString }
func (f *flagDef[T, P]) Placeholder() string     { return f.placeholder }
func (f *flagDef[T, P]) PosName() string         { return f.posName }
func (f *flagDef[T, P]) DashNumber()             { f.dashable |= DashNumber }
func (f *flagDef[T, P]) setPosName(n string)     { f.posName = n }
func (f *flagDef[T, P]) consumes() Consumes      { return f.consumption }
func (f *flagDef[T, P]) dashes() Dash            { return f.dashable }
func (f *flagDef[T, P]) setDashes(d Dash)        { f.dashable = d }
func (f *flagDef[T, P]) setParser(p any)         { f.parser = p.(parser[P]) }
func (f *flagDef[T, P]) assigned() bool          { return f.set }
func (f *flagDef[T, P]) reset()                  { f.set = false }
func (f *flagDef[T, P]) See() []*command         { return slices.Clone(f.seeCmds) }
func (f *flagDef[T, P]) setSeeCmds(c []*command) { f.seeCmds = c }
func (f *flagDef[T, P]) setDefault(val T, repr string) {
	*f.p = val
	f.defaultValue, f.defaultString, f.hasDefault = val, repr, true
	if repr == "" {
		f.defaultString = fmt.Sprintf(`"%v"`, val)
	}
}

func (f *flagDef[T, P]) Value() T {
	if !f.set && f.hasDefault {
		return f.defaultValue
	}
	return *f.p
}

func Toggle[T ~bool](name, description string) *toggle[T] {
	var dest T
	return ToggleVar(&dest, name, description)
}

func ToggleVar[T ~bool](p *T, name, description string) *toggle[T] {
	return &toggle[T]{flagDef[T, T]{p: p, name: name, description: description}}
}

func (t *toggle[T]) Default(val T, repr string) *toggle[T] {
	t.setDefault(val, repr)
	return t
}

func (t *toggle[T]) Hint(placeholder string) *toggle[T] {
	t.placeholder = placeholder
	return t
}

func (t *toggle[T]) Parse(string) (int, error) {
	*t.p = !*t.p
	t.set = true
	return 0, nil
}

func (t *toggle[T]) FlagOn(cmds ...Command) *toggle[T] {
	return flagsOn(t, "--"+t.name, cmds)
}

func (t *toggle[T]) FlagsOn(flags string, cmds ...Command) *toggle[T] {
	return flagsOn(t, flags, cmds)
}

type toggle[T ~bool] struct {
	flagDef[T, T]
}

type adder interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

func Add[T adder](by T, name, description string) *add[T] {
	var dest T
	return AddVar(&dest, by, name, description)
}

func AddVar[T adder](p *T, by T, name, description string) *add[T] {
	return &add[T]{
		flagDef: flagDef[T, T]{p: p, name: name, description: description},
		incr:    by,
	}
}

func (a *add[T]) Default(val T, repr string) *add[T] {
	a.setDefault(val, repr)
	return a
}

func (a *add[T]) Parse(string) (int, error) {
	*a.p += a.incr
	a.set = true
	return 0, nil
}

func (a *add[T]) FlagOn(cmds ...Command) *add[T] {
	return flagsOn(a, "--"+a.name, cmds)
}

func (a *add[T]) FlagsOn(flags string, cmds ...Command) *add[T] {
	return flagsOn(a, flags, cmds)
}

type add[T adder] struct {
	flagDef[T, T]
	incr T
}

// Option names and describes an option, and applies any specified settings.
//
// Settings must include a parser for types without built-in support.
func Option[T any](name, description string, settings ...FlagSetting) *option[T] {
	var dest T
	return OptionVar(&dest, name, description, settings...)
}

func OptionVar[T any](p *T, name, description string, settings ...FlagSetting) *option[T] {
	return parserOptionVar(p, name, description, parserFor[T](), settings...)
}

// StringOption is like Option, but doesn't require an explicit parser for string-like parsing of non-string.
func StringOption[T ~string](name, description string, settings ...FlagSetting) *option[T] {
	var dest T
	return parserOptionVar(&dest, name, description, parseString[T], settings...)
}

// IntOption is like Option, but doesn't require an explicit parser for int-like parsing of non-int.
func IntOption[T ~int | ~int8 | ~int16 | ~int32 | ~int64](name, description string, settings ...FlagSetting) *option[T] {
	var dest T
	return parserOptionVar(&dest, name, description, parseInt[T], settings...)
}

// UintOption is like Option, but doesn't require an explicit parser for uint-like parsing of non-uint.
func UintOption[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](name, description string, settings ...FlagSetting) *option[T] {
	var dest T
	return parserOptionVar(&dest, name, description, parseUint[T], settings...)
}

// FloatOption is like Option, but doesn't require an explicit parser for float-like parsing of non-float.
func FloatOption[T ~float32 | ~float64](name, description string, settings ...FlagSetting) *option[T] {
	var dest T
	return parserOptionVar(&dest, name, description, parseFloat[T], settings...)
}

// BoolOption is like Option, but doesn't require an explicit parser for bool-like parsing of non-bool.
func BoolOption[T ~bool](name, description string, settings ...FlagSetting) *option[T] {
	var dest T
	return parserOptionVar(&dest, name, description, parseBool[T], settings...)
}

func parserOptionVar[T any](p *T, name, description string, parser parser[T], settings ...FlagSetting) *option[T] {
	return &option[T]{
		flagDef: applyFlagSettings(flagDef[T, T]{p: p, name: name, description: description, parser: parser}, settings),
	}
}

func Enumerated[T comparable](name, description string, values ...T) *option[T] {
	var dest T
	return EnumeratedVar(&dest, name, description, values...)
}

func EnumeratedVar[T comparable](p *T, name, description string, values ...T) *option[T] {
	enum := make(map[T]struct{}, len(values))
	for _, v := range values {
		enum[v] = struct{}{}
	}
	o := OptionVar(p, name, description)
	o.validate = func(v T) error {
		if _, ok := enum[v]; ok {
			return nil
		}
		return ErrNotInEnum[T]{name, v, values}
	}
	return o
}

func (o *option[T]) Parse(name, value string) (int, error) {
	v, err := o.parser(value)
	if err != nil {
		return 0, err
	}
	if o.validate != nil {
		if err := o.validate(v); err != nil {
			return 0, err
		}
	}
	*o.p = v
	o.set = true
	return 1, nil
}

func (o *option[T]) Default(val T, repr string) *option[T] {
	o.setDefault(val, repr)
	return o
}

func (o *option[T]) Hint(placeholder string) *option[T] {
	o.placeholder = placeholder
	return o
}

func (o *option[T]) FlagOn(cmds ...Command) *option[T] {
	return flagsOn(o, "--"+o.name, cmds)
}

func (o *option[T]) FlagsOn(flags string, cmds ...Command) *option[T] {
	return flagsOn(o, flags, cmds)
}

func (o *option[T]) PosOn(name string, cmds ...Command) *option[T] {
	return posOn(o, name, cmds)
}

type option[T any] struct {
	flagDef[T, T]
	validate func(T) error
}

func Multi[T any](name, description string, settings ...FlagSetting) *multi[T] {
	var dest []T
	return MultiVar(&dest, name, description, settings...)
}

func MultiVar[T any](p *[]T, name, description string, settings ...FlagSetting) *multi[T] {
	return &multi[T]{
		flagDef: applyFlagSettings(flagDef[[]T, T]{
			p: p, name: name, description: description, consumption: ConsumesSlice,
			parser: parserFor[T](),
		}, settings),
	}
}

func (m *multi[T]) Default(val []T, repr string) *multi[T] {
	m.setDefault(val, repr)
	return m
}

func (m *multi[T]) Hint(placeholder string) *multi[T] {
	m.placeholder = placeholder
	return m
}

func (m *multi[T]) Parse(args []string) (int, error) {
	if len(args) < 1 {
		return 1, ErrMissingArgument
	}
	*m.p = make([]T, len(args))
	for i := 0; i < len(args); i++ {
		v, err := m.parser(args[i])
		if err != nil {
			return i - 1, err
		}
		(*m.p)[i] = v
	}
	m.set = true
	return len(args), nil
}

func (m *multi[T]) PosOn(name string, cmds ...Command) *multi[T] {
	return posOn(m, name, cmds)
}

type multi[T any] struct {
	flagDef[[]T, T]
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

func flagsOn[T Flag](f T, flags string, cmds []Command) T {
	for _, cmd := range cmds {
		cmd.addFlag(flags, f)
	}
	return f
}

func posOn[T Flag](f T, name string, cmds []Command) T {
	if name != "" {
		f.setPosName(name)
	}
	for _, cmd := range cmds {
		cmd.addPos(f)
	}
	return f
}

func isNumeric(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}
