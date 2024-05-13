package call

import "fmt"

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

type Dash uint

const (
	DashNone   Dash = 0
	DashNumber Dash = 1 << iota
)

func (d Dash) configureFlag(f Flag) {
	f.setDashes(d)
}

type defaulter[T any] struct {
	repr  string
	value T
}

func Default[T any](value T) defaulter[T]                  { return DefaultRepr(value, fmt.Sprint(value)) }
func DefaultRepr[T any](value T, repr string) defaulter[T] { return defaulter[T]{repr, value} }

func (d defaulter[T]) configureFlag(f Flag) {
	type defaultingFlag = interface{ setDefault(T, string) }
	f.(defaultingFlag).setDefault(d.value, d.repr)
}

type parser[T any] func(string) (T, error)

func (p parser[T]) configureFlag(f Flag) {
	type parsingFlag = interface{ setParser(parser[T]) }
	f.(parsingFlag).setParser(p)
}

// Parser specifies a parsing function for this flag.
// This can be used for types without default parsers, such as to accept enum names on an integer values.
func Parser[T any](p parser[T]) parser[T] { return parser[T](p) }
