package call

import (
	"errors"
	"strconv"
)

var ErrMissingArgument = errors.New("missing")

type Flag interface {
	ID() (string, string)
	Parse(args []string) (consumed int, err error)
	assigned() bool
	reset()
}

func Toggle[T ~bool](name, description string) *toggle[T] {
	var dest T
	return ToggleVar(&dest, name, description)
}

func ToggleVar[T ~bool](p *T, name, description string) *toggle[T] {
	return &toggle[T]{p, false, name, description, strconv.ParseBool}
}

func (t *toggle[T]) ID() (string, string) { return t.Name, t.Description }
func (t *toggle[T]) Value() T             { return *t.p }
func (t *toggle[T]) assigned() bool       { return t.set }
func (t *toggle[T]) reset()               { t.set = false }
func (t *toggle[T]) Parse(args []string) (int, error) {
	*t.p = !*t.p
	t.set = true
	return 0, nil
}

func (t *toggle[T]) FlagOn(cmds ...Command) *toggle[T] {
	return t.FlagsOn("--"+t.Name, cmds...)
}

func (t *toggle[T]) FlagsOn(flags string, cmds ...Command) *toggle[T] {
	for _, cmd := range cmds {
		cmd.addFlag(flags, t)
	}
	return t
}

type toggle[T ~bool] struct {
	p           *T
	set         bool
	Name        string
	Description string
	parse       func(string) (bool, error)
}

type adder interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

func Add[T adder](by T, name, description string) *add[T] {
	var dest T
	return AddVar(&dest, by, name, description)
}

func AddVar[T adder](p *T, by T, name, description string) *add[T] {
	return &add[T]{p, by, false, name, description, strconv.ParseBool}
}

func (a *add[T]) ID() (string, string) { return a.Name, a.Description }
func (a *add[T]) Value() T             { return *a.p }
func (a *add[T]) assigned() bool       { return a.set }
func (a *add[T]) reset()               { a.set = false }
func (a *add[T]) Parse(args []string) (int, error) {
	*a.p += a.incr
	return 0, nil
}

func (a *add[T]) FlagOn(cmds ...Command) *add[T] {
	return a.FlagsOn("--"+a.Name, cmds...)
}

func (a *add[T]) FlagsOn(flags string, cmds ...Command) *add[T] {
	for _, cmd := range cmds {
		cmd.addFlag(flags, a)
	}
	return a
}

type add[T adder] struct {
	p           *T
	incr        T
	set         bool
	Name        string
	Description string
	parse       func(string) (bool, error)
}

func Option[T any](name, description string) *option[T] {
	var dest T
	return OptionVar(&dest, name, description)
}

func OptionVar[T any](p *T, name, description string) *option[T] {
	return &option[T]{p: p, Name: name, Description: description, parse: parserFor[T]()}
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

func (o *option[T]) ID() (string, string) { return o.Name, o.Description }
func (o *option[T]) Value() T {
	if !o.set && o.hasDefault {
		return o.fallback
	}
	return *o.p
}
func (o *option[T]) Default(v T) *option[T]        { o.fallback, o.hasDefault = v, true; return o }
func (o *option[T]) AcceptDash(ok bool) *option[T] { o.dashOK = ok; return o }
func (o *option[T]) acceptDash() bool              { return o.dashOK }
func (o *option[T]) assigned() bool                { return o.set }
func (o *option[T]) reset()                        { o.set = false }
func (o *option[T]) Parse(args []string) (int, error) {
	if len(args) < 1 {
		return 1, ErrMissingArgument
	}
	v, err := o.parse(args[0])
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

func (o *option[T]) FlagOn(cmds ...Command) *option[T] {
	return o.FlagsOn("--"+o.Name, cmds...)
}

func (o *option[T]) FlagsOn(flags string, cmds ...Command) *option[T] {
	for _, cmd := range cmds {
		cmd.addFlag(flags, o)
	}
	return o
}

func (o *option[T]) PosOn(cmds ...Command) *option[T] {
	for _, cmd := range cmds {
		cmd.addPos(o)
	}
	return o
}

type option[T any] struct {
	p           *T
	fallback    T
	dashOK      bool // accept -* when parsing as a positionl arg
	hasDefault  bool
	set         bool
	validate    func(T) error
	Name        string
	Description string
	parse       func(string) (T, error)
}

func Multi[T any](name, description string) *multi[T] {
	var dest []T
	return MultiVar(&dest, name, description)
}

func MultiVar[T any](p *[]T, name, description string) *multi[T] {
	return &multi[T]{p, false, name, description, parserFor[T]()}
}

func (m *multi[T]) ID() (string, string) { return m.Name, m.Description }
func (m *multi[T]) Value() []T           { return *m.p }
func (m *multi[T]) assigned() bool       { return m.set }
func (m *multi[T]) reset()               { m.set = false }
func (m *multi[T]) Parse(args []string) (int, error) {
	if len(args) < 1 {
		return 1, ErrMissingArgument
	}
	*m.p = make([]T, len(args))
	for i := 0; i < len(args); i++ {
		v, err := m.parse(args[i])
		if err != nil {
			return i - 1, err
		}
		(*m.p)[i] = v
	}
	m.set = true
	return len(args), nil
}

func (a *multi[T]) PosOn(cmds ...Command) *multi[T] {
	for _, cmd := range cmds {
		cmd.addPos(a)
	}
	return a
}

type multi[T any] struct {
	p           *[]T
	set         bool
	Name        string
	Description string
	parse       func(string) (T, error)
}

func parserFor[T any]() func(string) (T, error) {
	var zero T
	return map[any]any{
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
		false:      strconv.ParseBool,
		"":         parseString[string],
	}[zero].(func(string) (T, error))
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
