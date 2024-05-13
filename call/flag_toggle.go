package call

func Toggle[T ~bool](name, description string, settings ...FlagSetting) *toggle[T] {
	var dest T
	return ToggleVar(&dest, name, description, settings...)
}

func ToggleVar[T ~bool](p *T, name, description string, settings ...FlagSetting) *toggle[T] {
	return &toggle[T]{applyFlagSettings(flagDef[T, T]{p: p, name: name, description: description, parser: noParse[T]}, settings)}
}

func (t *toggle[T]) Hint(placeholder string) *toggle[T] {
	t.placeholder = placeholder
	return t
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
