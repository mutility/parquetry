package run

import "context"

type Handler func(context.Context, Environ) error

func (h Handler) applyCommand(cmd *Command) error {
	return cmd.Runs(h)
}

// Handler2 adapts a func(Context, Environ, T1, T2) for (*command).Runs.
func Handler2[T1 any, T2 any](
	handler func(context.Context, Environ, T1, T2) error, f1, f2 *Option,
) Handler {
	return func(ctx context.Context, env Environ) error {
		return handler(ctx, env, f1.Any().(T1), f2.Any().(T2))
	}
}

// Handler6 adapts a func(Context, Environ, T1...T6) for (*command).Runs.
func Handler6[T1 any, T2 any, T3 any, T4 any, T5 any, T6 any](
	handler func(context.Context, Environ, T1, T2, T3, T4, T5, T6) error, f1, f2, f3, f4, f5, f6 *Option,
) Handler {
	return func(ctx context.Context, env Environ) error {
		return handler(ctx, env, f1.Any().(T1), f2.Any().(T2), f3.Any().(T3), f4.Any().(T4), f5.Any().(T5), f6.Any().(T6))
	}
}
