package call

import "context"

type flag[T any] interface{ Value() T }

// Handler1 adapts a func(Context, Environ, T1) for (*command).Runs.
// T1 is pulled from the specified Flag, Harcoded, or Singleton.
func Handler1[T1 any, F1 flag[T1]](
	handler func(context.Context, Environ, T1) error, f1 F1,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value())
	}
}

// Handler2 adapts a func(Context, Environ, T1, T2) for (*command).Runs.
// T1, T2 are pulled from the specified Flags, Harcodeds, or Singletons.
func Handler2[
	T1 any, F1 flag[T1],
	T2 any, F2 flag[T2],
](
	handler func(context.Context, Environ, T1, T2) error, f1 F1, f2 F2,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value())
	}
}

// Handler3 adapts a func(Context, Environ, T1...T3) for (*command).Runs.
// T1...T3 are pulled from the specified Flags, Harcodeds, or Singletons.
func Handler3[
	T1 any, F1 flag[T1],
	T2 any, F2 flag[T2],
	T3 any, F3 flag[T3],
](
	handler func(context.Context, Environ, T1, T2, T3) error, f1 F1, f2 F2, f3 F3,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value())
	}
}

// Handler4 adapts a func(Context, Environ, T1...T4) for (*command).Runs.
// T1...T4 are pulled from the specified Flags, Harcodeds, or Singletons.
func Handler4[
	T1 any, F1 flag[T1],
	T2 any, F2 flag[T2],
	T3 any, F3 flag[T3],
	T4 any, F4 flag[T4],
](
	handler func(context.Context, Environ, T1, T2, T3, T4) error, f1 F1, f2 F2, f3 F3, f4 F4,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value(), f4.Value())
	}
}

// Handler5 adapts a func(Context, Environ, T1...T5) for (*command).Runs.
// T1...T5 are pulled from the specified Flags, Harcodeds, or Singletons.
func Handler5[
	T1 any, F1 flag[T1],
	T2 any, F2 flag[T2],
	T3 any, F3 flag[T3],
	T4 any, F4 flag[T4],
	T5 any, F5 flag[T5],
](
	handler func(context.Context, Environ, T1, T2, T3, T4, T5) error, f1 F1, f2 F2, f3 F3, f4 F4, f5 F5,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value(), f4.Value(), f5.Value())
	}
}

// Handler6 adapts a func(Context, Environ, T1...T6) for (*command).Runs.
// T1...T6 are pulled from the specified Flags, Harcodeds, or Singletons.
func Handler6[
	T1 any, F1 flag[T1],
	T2 any, F2 flag[T2],
	T3 any, F3 flag[T3],
	T4 any, F4 flag[T4],
	T5 any, F5 flag[T5],
	T6 any, F6 flag[T6],
](
	handler func(context.Context, Environ, T1, T2, T3, T4, T5, T6) error, f1 F1, f2 F2, f3 F3, f4 F4, f5 F5, f6 F6,
) func(context.Context, Environ) error {
	return func(ctx context.Context, e Environ) error {
		return handler(ctx, e, f1.Value(), f2.Value(), f3.Value(), f4.Value(), f5.Value(), f6.Value())
	}
}

// Hardcoded adapts an unchanging value for a HandlerN call.
// This can make it easier to share run functions across commands with different sets of flags.
func Hardcoded[T any](v T) hardcoded[T] { return hardcoded[T]{v} }

type hardcoded[T any] struct{ v T }

func (v hardcoded[T]) Value() T { return v.v }

// Singleton adapts a single valued flag to a Slice parameter.
// This can make it easier to share run functions across commands with different sets of flags.
func Singleton[T any, FT flag[T]](v FT) singleton[T, FT] { return singleton[T, FT]{v} }

type singleton[T any, FT flag[T]] struct{ f FT }

func (v singleton[T, FT]) Value() []T { return []T{v.f.Value()} }
