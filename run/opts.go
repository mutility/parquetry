package run

func FileSlice(name, desc string) Option {
	return FileLikeSlice[string](name, desc)
}

func FileLikeSlice[T ~string](name, desc string) Option {
	var v stores[T]
	parse := func(s string) error {
		v.Add(T(s))
		return nil
	}
	return Option{
		name:      name,
		desc:      desc,
		storage:   &v,
		parseMany: manyParser(parse),
		strOK:     []string{"-"},
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

type stores[T any] struct{ value []T }

func (s *stores[T]) Any() any   { return s.value }
func (s *stores[T]) Value() []T { return s.value }
func (s *stores[T]) Add(v T)    { s.value = append(s.value, v) }
func (s *stores[T]) Set(v []T)  { s.value = v }
