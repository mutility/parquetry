package run

type options[T any] struct {
	name     string
	desc     string
	value    *[]T
	parse    func(string) error
	prefixOK string   // set to - to allow -[^-]+, or -- to allow --.+ in arg context
	strOK    []string // include unusual values such as - to allow them in arg context
	see      []*Command
}

func (o *options[T]) description() string            { return o.desc }
func (o *options[T]) seeAlso() []*Command            { return o.see }
func (o *options[T]) setSeeAlso(cmds ...*Command)    { o.see = cmds }
func (o *options[T]) parseDefault(arg string) error  { return o.parse(arg) }
func (o *options[T]) parseMany(args ArgSource) error { return manyParser(o.parse)(args) }
func (o *options[T]) okValues() []string             { return o.strOK }
func (o *options[T]) okPrefix() string               { return o.prefixOK }

func (o *options[T]) Value() []T { return *o.value }

// Rest returns an multi-Arg definition for this option with a custom alias.
func (o *options[T]) Rest(name string) Arg {
	return Arg{option: o, name: name, many: true}
}

func FileSlice(name, desc string) options[string] {
	return FileLikeSlice[string](name, desc)
}

func FileLikeSlice[T ~string](name, desc string) options[T] {
	var v []T
	parse := func(s string) error {
		v = append(v, T(s))
		return nil
	}
	return options[T]{
		name:  name,
		desc:  desc,
		value: &v,
		parse: parse,
		strOK: []string{"-"},
	}
}

func manyParser(parse func(string) error) func(ArgSource) error {
	return func(s ArgSource) error {
		for v, ok := s.PeekMany(); ok; v, ok = s.PeekMany() {
			if !ok {
				return missingArgsError{}
			}
			if err := parse(v); err != nil {
				return err
			}
			s.Next()
		}
		return nil
	}
}
