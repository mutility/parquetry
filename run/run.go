package run

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

var (
	ErrRedefined = errors.New("already set")
	ErrMissing   = errors.New("missing")
)

func App(name, desc string) *Application {
	return &Application{
		Command: Command{
			name: name,
			desc: desc,
		},
	}
}

func AppOpt(name, desc string, opts ...CmdOption) (*Application, error) {
	app := App(name, desc)
	return app, applyOpts(&app.Command, opts)
}

type Application struct {
	Command

	allowGroupShortFlags bool
}

func (a *Application) Main(ctx context.Context) int {
	env := DefaultEnviron()
	if cmd, err := a.main(ctx, env); err != nil {
		a.Ferror(env.Stderr, err)
		return exitCode(err)
	} else if err = cmd.run(ctx, env); err != nil {
		a.Ferror(env.Stderr, err)
		return exitCode(err)
	}
	return 0
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ec interface{ ExitCode() int }
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	return 1
}

func (a *Application) AllowGroupShortFlags(f bool) {
	a.allowGroupShortFlags = f
}

func (a *Application) Ferror(w io.Writer, err error) {
	fmt.Fprintf(w, "%s: error: %v\n", a.Name(), err)
}

func (a *Application) MainEnv(ctx context.Context, env Environ) error {
	env.fillDefaults()
	cmd, err := a.main(ctx, env)
	switch err := err.(type) {
	case extraArgsError:
		err.cmd.PrintHelp(ctx, env, a)
	case missingArgsError:
		err.cmd.PrintHelp(ctx, env, a)
	}
	if err != nil {
		return err
	}
	if len(cmd.cmds) == 0 || cmd.handler != nil {
		return cmd.run(ctx, env)
	}
	return missingCmdError{cmd}
}

func (a *Application) ParseEnv(ctx context.Context, env Environ) (*Command, error) {
	env.fillDefaults()
	return a.main(ctx, env)
}

func (a *Application) main(ctx context.Context, env Environ) (*Command, error) {
	if len(env.Args) < 1 {
		return nil, wrap(ErrMissing, "program name")
	}

	arg0 := env.Args[0]
	_ = arg0

	cur := &a.Command
	canFlag := true
	carg := 0
	showHelp := false

	maybeFlag := func(arg string) bool { return strings.HasPrefix(arg, "--") || (len(arg) == 2 && arg[0] == '-') }
	if a.allowGroupShortFlags {
		maybeFlag = func(arg string) bool { return strings.HasPrefix(arg, "-") }
	}

	for i := 1; i < len(env.Args); {
		arg := env.Args[i]
		if canFlag {
			if arg == "--" {
				canFlag = false
				i++
				continue
			}

			if idx, rem := cur.lookupFlag(arg); idx >= 0 && canFlag {
				opt := &cur.flags[idx]
				args := flagArgs{env.Args, &i, rem, rem > 0}
				if rem == 0 {
					i++
				}
				if err := opt.option.(optionFlagArg).parseArg(&args); err != nil {
					return nil, flagParseError{cur, opt, arg, err}
				}
				if args.needConsume {
					return nil, flagArgUnconsumedError{cur, arg, rem}
				}
				opt.valueSet = true
				continue
			}

			if !cur.noHelp && (arg == "-h" || arg == "--help") {
				showHelp = true
				i++
				continue
			}

			if maybeFlag(arg) && (carg >= len(cur.args) || !cur.args[carg].can(arg)) {
				return nil, extraFlagError{cur, arg}
			}
		}

		if carg < len(cur.args) {
			opt := &cur.args[carg]
			args := argArgs{opt, env.Args, &i, true, &canFlag, maybeFlag}
			if m, ok := opt.option.(optionSlice); ok {
				if err := m.parseMany(&args); err != nil {
					return nil, argParseError{cur, opt, arg, err}
				}
			} else if err := opt.option.(optionFlagArg).parseArg(&args); err != nil {
				return nil, argParseError{cur, opt, arg, err}
			}
			if args.needConsume {
				return nil, argUnconsumedError{cur, arg}
			}
			carg++
			continue
		}

		if idx := cur.lookupCmd(arg); idx >= 0 {
			cmd := cur.cmds[idx]
			cmd.parent = cur
			cur = cmd
			carg = 0
			i++
			continue
		}

		return nil, extraArgsError{cur, env.Args[i:]}
	}

	if showHelp {
		if cur.noHelp {
			return nil, HelpDisabledError{cur}
		}
		return helpCommand(a, cur), nil
	}

	if carg < len(cur.args) {
		return nil, missingArgsError{cur, cur.args[carg:]}
	}

	for cmd := cur; cmd != nil; cmd = cmd.parent {
		for f := range cmd.flags {
			flag := &cmd.flags[f]
			if flag.defaultSet && !flag.valueSet {
				err := flag.option.parseDefault(flag.defaultString)
				if err != nil {
					return cur, flagParseError{cur, flag, flag.defaultString, err}
				}
			}
		}
	}

	return cur, nil
}

type flagArgs struct {
	args        []string
	pi          *int // increment to consume additional args
	rem         int  // if nonzero, first arg contains a value starting index rem
	needConsume bool // tracks whether embedded value was consumed
}

func (a *flagArgs) Peek() (string, bool) {
	if *a.pi >= len(a.args) || (a.rem > 0 && !a.needConsume) {
		return "", false
	}
	if a.needConsume {
		return a.args[*a.pi][a.rem:], true
	} else {
		return a.args[*a.pi], true
	}
}

func (a *flagArgs) Next() (string, bool) {
	v, ok := a.Peek()
	if ok {
		*a.pi++
		a.needConsume = false
	}
	return v, ok
}

type argArgs struct {
	arg         *Arg
	args        []string
	pi          *int  // increment to consume additional args
	needConsume bool  // tracks whether one or more args were consumed
	pcanFlag    *bool // tracks whether we've seen --
	maybeFlag   func(string) bool
}

func (a *argArgs) Peek() (string, bool) {
	if *a.pi >= len(a.args) {
		return "", false
	}
	return a.args[*a.pi], true
}

func (a *argArgs) PeekMany() (string, bool) {
	if *a.pi >= len(a.args) {
		return "", false
	}
	arg := a.args[*a.pi]
	if *a.pcanFlag {
		if arg == "--" {
			*a.pcanFlag = false
			*a.pi++ // peek normally doesn't advance, but will skip --
			return a.PeekMany()
		}

		if a.maybeFlag(arg) && !a.arg.can(arg) {
			return "", false
		}
	}

	return a.args[*a.pi], true
}

func (a *argArgs) Next() (string, bool) {
	v, ok := a.Peek()
	if ok {
		*a.pi++
		a.needConsume = false
	}
	return v, ok
}

func wrap(e error, m string) error {
	if e == nil {
		return e
	}
	return fmt.Errorf("%s: %w", m, e)
}
