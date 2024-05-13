package call

import "slices"

type option[T any] struct {
	flagDef[T, T]
	prevalidate  func(string) error
	postvalidate func(T) error
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

// EnumRaw specifies that passed value must match one of the provided strings.
func (o *option[T]) EnumRaw(values ...string) *option[T] {
	o.prevalidate = func(got string) error {
		if slices.Contains(values, got) {
			return nil
		}
		return ErrNotInEnum[string]{o.name, got, values}
	}
	return o
}

// EnumFunc specifies that parsed value must match one of the provided values.
func (o *option[T]) EnumFunc(compare func(T, T) int, values ...T) *option[T] {
	// bluesky: the equals func could be elided if T could be constrained
	o.postvalidate = func(got T) error {
		if slices.ContainsFunc(values, func(e T) bool { return compare(e, got) == 0 }) {
			return nil
		}
		return ErrNotInEnum[T]{o.name, got, values}
	}
	return o
}

// EnumMap specifies a mapping from known string to known value. Others are disallowed.
func (o *option[T]) EnumMap(namedValues map[string]T) *option[T] {
	names := make([]string, 0, len(namedValues))
	values := make([]T, 0, len(namedValues))
	for n, v := range namedValues {
		names = append(names, n)
		values = append(values, v)
	}
	o.parser = func(name string) (T, error) {
		if i := slices.Index(names, name); i >= 0 {
			return values[i], nil
		}
		var zero T
		return zero, ErrNotInEnum[string]{o.name, name, names}
	}
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
