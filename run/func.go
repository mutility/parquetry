package run

import "context"

type Handler func(context.Context, Environ) error

func (h Handler) applyCommand(cmd *Command) error {
	return cmd.Runs(h)
}

// Handler2 adapts a func(Context, Environ, T1, T2) for (*command).Runs.
func Handler2[
	T1 any, V1 Param[T1],
	T2 any, V2 Param[T2],
](
	handler func(context.Context, Environ, T1, T2) error,
	v1 V1, v2 V2,
) Handler {
	return func(ctx context.Context, env Environ) error {
		return handler(ctx, env, v1.Value(), v2.Value())
	}
}

type Param[T any] interface{ Value() T }

// Handler6 adapts a func(Context, Environ, T1...T6) for (*command).Runs.
func Handler6[
	T1 any, V1 Param[T1],
	T2 any, V2 Param[T2],
	T3 any, V3 Param[T3],
	T4 any, V4 Param[T4],
	T5 any, V5 Param[T5],
	T6 any, V6 Param[T6],
](
	handler func(context.Context, Environ, T1, T2, T3, T4, T5, T6) error,
	v1 V1, v2 V2, v3 V3, v4 V4, v5 V5, v6 V6,
) Handler {
	return func(ctx context.Context, env Environ) error {
		return handler(ctx, env, v1.Value(), v2.Value(), v3.Value(), v4.Value(), v5.Value(), v6.Value())
	}
}
