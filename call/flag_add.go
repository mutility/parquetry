package call

type adder interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

type add[T adder] struct {
	flagDef[T, T]
	incr T
}

func Add[T adder](by T, name, description string, settings ...FlagSetting) *add[T] {
	var dest T
	return AddVar(&dest, by, name, description, settings...)
}

func AddVar[T adder](p *T, by T, name, description string, settings ...FlagSetting) *add[T] {
	return &add[T]{
		flagDef: applyFlagSettings(flagDef[T, T]{p: p, name: name, description: description, parser: noParse[T]}, settings),
		incr:    by,
	}
}

func (a *add[T]) FlagOn(cmds ...Command) *add[T] {
	return flagsOn(a, "--"+a.name, cmds)
}

func (a *add[T]) FlagsOn(flags string, cmds ...Command) *add[T] {
	return flagsOn(a, flags, cmds)
}
