package call

type multi[T any] struct {
	flagDef[[]T, T]
}

func Multi[T any](name, description string, settings ...FlagSetting) *multi[T] {
	var dest []T
	return MultiVar(&dest, name, description, settings...)
}

func MultiVar[T any](p *[]T, name, description string, settings ...FlagSetting) *multi[T] {
	return &multi[T]{
		flagDef: applyFlagSettings(flagDef[[]T, T]{
			p: p, name: name, description: description,
			parser: parserFor[T](),
		}, settings),
	}
}

func (m *multi[T]) Hint(placeholder string) *multi[T] {
	m.placeholder = placeholder
	return m
}

func (m *multi[T]) PosOn(name string, cmds ...Command) *multi[T] {
	return posOn(m, name, cmds)
}

func (m *multi[T]) describe(base string) string { return base + " ..." }
