package main

import (
	"reflect"
	"slices"
	"time"

	"github.com/expr-lang/expr"
)

func filterWrite(filter Filter, rowType reflect.Type, w WriteFunc) (WriteFunc, error) {
	if filter == "" {
		return w, nil
	}
	env := reflect.New(rowType).Elem().Interface()

	match, err := expr.Compile(
		string(filter),
		slices.Concat(
			[]expr.Option{
				expr.Env(env),
				expr.AsBool(),
				expr.Timezone("UTC"),
			},
			typeCompare[Date, time.Time](epochCompare),
			typeCompare[TimeMilliUTC, time.Duration](timeCompare),
			typeCompare[TimeMicroUTC, time.Duration](timeCompare),
			typeCompare[TimeNanoUTC, time.Duration](timeCompare),
			typeCompare[StampMilliUTC, time.Time](epochCompare),
			typeCompare[StampMicroUTC, time.Time](epochCompare),
			typeCompare[StampNanoUTC, time.Time](epochCompare),
		)...,
	)
	if err != nil {
		return w, err
	}
	return func(v reflect.Value) error {
		if include, err := expr.Run(match, v.Interface()); err != nil {
			return err
		} else if include.(bool) {
			return w(v)
		}
		return nil
	}, nil
}

func typeCompare[T inttime, U any](cmp func(T, any) (int, error)) []expr.Option {
	relate := func(op, name string, is func(int) bool) []expr.Option {
		return []expr.Option{
			expr.Operator(op, op+name, name+op),
			expr.Function(op+name,
				func(params ...any) (any, error) {
					rel, err := cmp(params[0].(T), params[1])
					return is(rel), err
				},
				new(func(T, int) bool),
				new(func(T, string) bool),
				new(func(T, U) bool),
				new(func(T, T) bool),
			),
			expr.Function(name+op,
				func(params ...any) (any, error) {
					rel, err := cmp(params[1].(T), params[0])
					return is(-rel), err
				},
				new(func(int, T) bool),
				new(func(string, T) bool),
				new(func(U, T) bool),
			),
		}
	}

	ty := reflect.TypeFor[T]().String()
	return slices.Concat(
		relate("==", ty, func(n int) bool { return n == 0 }),
		relate("!=", ty, func(n int) bool { return n != 0 }),
		relate("<", ty, func(n int) bool { return n < 0 }),
		relate("<=", ty, func(n int) bool { return n <= 0 }),
		relate(">", ty, func(n int) bool { return n > 0 }),
		relate(">=", ty, func(n int) bool { return n >= 0 }),
	)
}
