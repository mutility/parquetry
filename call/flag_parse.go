package call

func (t *toggle[T]) parseFlag(string) (int, error) {
	*t.p = !*t.p
	t.set = true
	return 0, nil
}

func (t *toggle[T]) parseName() error {
	*t.p = !*t.p
	t.set = true
	return nil
}

func (a *add[T]) parseFlag(string) (int, error) {
	*a.p += a.incr
	a.set = true
	return 0, nil
}

func (a *add[T]) parseName() error {
	*a.p += a.incr
	a.set = true
	return nil
}

func (o *option[T]) parseFlag(value string) (int, error) {
	if err := validate(o.prevalidate, value); err != nil {
		return 0, err
	}
	v, err := o.parser(value)
	if err != nil {
		return 0, err
	}
	if err := validate(o.postvalidate, v); err != nil {
		return 0, err
	}
	*o.p = v
	o.set = true
	return 1, nil
}

func (o *option[T]) parsePosition(name string, args []string) (int, error) {
	return o.parseFlag(args[0])
}

func (m *multi[T]) parsePosition(name string, args []string) (int, error) {
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
