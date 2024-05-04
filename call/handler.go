package call

import "context"

type flag[T any] interface{ Value() T }

func Handler2[T1, T2 any,
	F1 flag[T1], F2 flag[T2],
](
	handler func(context.Context, Environ, T1, T2) error, f1 F1, f2 F2,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value())
	}
}

func Handler3[T1, T2, T3 any,
	F1 flag[T1], F2 flag[T2], F3 flag[T3],
](
	handler func(context.Context, Environ, T1, T2, T3) error, f1 F1, f2 F2, f3 F3,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value())
	}
}

func Handler4[T1, T2, T3, T4 any,
	F1 flag[T1], F2 flag[T2], F3 flag[T3], F4 flag[T4],
](
	handler func(context.Context, Environ, T1, T2, T3, T4) error, f1 F1, f2 F2, f3 F3, f4 F4,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value(), f4.Value())
	}
}

func Handler5[T1, T2, T3, T4, T5 any,
	F1 flag[T1], F2 flag[T2], F3 flag[T3], F4 flag[T4], F5 flag[T5],
](
	handler func(context.Context, Environ, T1, T2, T3, T4, T5) error, f1 F1, f2 F2, f3 F3, f4 F4, f5 F5,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value(), f4.Value(), f5.Value())
	}
}

func Handler6[T1, T2, T3, T4, T5, T6 any,
	F1 flag[T1], F2 flag[T2], F3 flag[T3], F4 flag[T4], F5 flag[T5], F6 flag[T6],
](
	handler func(context.Context, Environ, T1, T2, T3, T4, T5, T6) error, f1 F1, f2 F2, f3 F3, f4 F4, f5 F5, f6 F6,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value(), f4.Value(), f5.Value(), f6.Value())
	}
}

func Hardcoded[T any](v T) hardcoded[T] { return hardcoded[T]{v} }

type hardcoded[T any] struct{ v T }

func (v hardcoded[T]) Value() T { return v.v }
