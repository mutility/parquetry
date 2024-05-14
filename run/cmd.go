package run

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
)

type Runner interface {
	Run(context.Context, *Orders) error
}

type RunFunc func(context.Context, *Orders) error

func (r RunFunc) Run(ctx context.Context, o *Orders) error { return r(ctx, o) }

type Orders struct{}

type Flag struct {
	rune          rune
	string        string
	option        Option
	hint          string
	defaultString string
	defaultSet    bool
	valueSet      bool
}

// Default specifies a value that will be supplied for an unprovided flag.
func (f Flag) Default(string string) Flag {
	f.defaultString = string
	f.defaultSet = true
	return f
}

type flags []Flag

func (f flags) searchRune(index int, r rune) int     { return cmp.Compare(f[index].rune, r) }
func (f flags) searchString(index int, s string) int { return cmp.Compare(f[index].string, s) }

type commands []*Command

func (c commands) searchString(index int, s string) int { return cmp.Compare(c[index].name, s) }

type Arg struct {
	option Option
	name   string
	many   bool
}

func (a Arg) can(dashArg string) (ok bool) {
	if slices.Contains(a.option.okValues(), dashArg) {
		return true
	}

	nonDash := strings.IndexFunc(dashArg, func(r rune) bool { return r != '-' })
	return nonDash >= 0 && strings.HasPrefix(a.option.okPrefix(), dashArg[:nonDash])
}

func (a Arg) describe() string {
	desc := "<" + a.name + ">"
	if a.many {
		desc += " ..."
	}
	return desc
}

func Cmd(name, desc string) *Command {
	return &Command{name: name, desc: desc}
}

func CmdOpt(name, desc string, opts ...CmdOption) (*Command, error) {
	cmd := Cmd(name, desc)
	return cmd, applyOpts(cmd, opts)
}

type Command struct {
	name, desc, detail string

	parent *Command
	cmds   commands
	flags  flags
	args   []Arg

	clookup func(arg string) int              // returns index in cmds of matching *Command (or -1)
	flookup func(arg string) (index, rem int) // returns index in flags of matching flag (or -1), index in arg after = (or 0)

	handler func(context.Context, Environ) error
	noHelp  bool
}

func (c *Command) Runs(handler func(context.Context, Environ) error) error {
	if c.handler != nil {
		return wrap(ErrRedefined, c.name+" handler")
	}
	c.handler = handler
	return nil
}

func (c *Command) CommandName() string {
	parts := []string{c.name}
	for c.parent != nil {
		c = c.parent
		parts = append(parts, c.name)
	}
	slices.Reverse(parts)
	return strings.Join(parts[1:], ".")
}

func (c *Command) Name() string {
	parts := []string{c.name}
	for c.parent != nil {
		c = c.parent
		parts = append(parts, c.name)
	}
	slices.Reverse(parts)
	return strings.Join(parts, ".")
}

func (c *Command) Details(detail string) error {
	if c.detail != "" {
		return wrap(ErrRedefined, c.name+" detail")
	}
	c.detail = detail
	return nil
}

func (c *Command) DetailsFor(detail string, opts ...Option) error {
	if c.detail != "" {
		return wrap(ErrRedefined, c.name+" detail")
	}
	c.detail = detail
	for _, opt := range opts {
		opt.setSeeAlso(c)
	}
	return nil
}

// lookupCmd returns the index of the matching *Command (or -1)
func (c *Command) lookupCmd(arg string) int {
	if c.clookup == nil {
		return -1
	}
	return c.clookup(arg)
}

func (c *Command) run(ctx context.Context, env Environ) error {
	if c.handler == nil {
		panic(c.name + ": not handled")
	}
	return c.handler(ctx, env)
}

// lookupFlag returns the index of the matching flag (or -1), and the index in arg after an = (or 0)
func (c *Command) lookupFlag(arg string) (index, rem int) {
	if c.flookup == nil {
		return -1, 0
	}
	return c.flookup(arg)
}

func (c *Command) Flags(flags ...Flag) error {
	if c.flookup != nil {
		return wrap(ErrRedefined, c.name+" flags")
	}
	c.flags = flags

	runeFlags, stringFlags := 0, 0
	for _, f := range flags {
		if f.rune != 0 {
			runeFlags++
		}
		if f.string != "" {
			stringFlags++
		}
	}

	runeIndex := make([]int, 0, runeFlags)
	stringIndex := make([]int, 0, stringFlags)

	for i, f := range flags {
		if f.rune != 0 {
			runeIndex = append(runeIndex, i)
		}
		if f.string != "" {
			stringIndex = append(stringIndex, i)
		}
	}
	slices.SortFunc(runeIndex, func(a, b int) int { return cmp.Compare(c.flags[a].rune, flags[b].rune) })
	slices.SortFunc(stringIndex, func(a, b int) int { return cmp.Compare(c.flags[a].string, flags[b].string) })

	c.flookup = func(arg string) (index, rem int) {
		switch {
		case len(arg) == 0 || arg[0] != '-' || arg == "-":
			return -1, 0
		case arg[1] == '-':
			pos, ok := slices.BinarySearchFunc(stringIndex, arg[2:], c.flags.searchString)
			if !ok {
				eq := strings.IndexByte(arg, '=')
				if eq >= 3 {
					rem = eq + 1
					pos, ok = slices.BinarySearchFunc(stringIndex, arg[2:eq], c.flags.searchString)
				}
			}
			if ok {
				return stringIndex[pos], rem
			}
		default:
			pos, ok := slices.BinarySearchFunc(runeIndex, ([]rune(arg))[1], c.flags.searchRune)
			if ok {
				return runeIndex[pos], 0
			}
		}

		return -1, 0
	}

	return nil
}

func (c *Command) Args(args ...Arg) error {
	if c.args != nil {
		return wrap(ErrRedefined, c.name+" args")
	}
	c.args = args
	return nil
}

func (c *Command) Commands(cmds ...*Command) error {
	if c.clookup != nil {
		return wrap(ErrRedefined, c.name+" commands")
	}
	c.cmds = cmds

	stringIndex := make([]int, 0, len(cmds))
	for i, sub := range cmds {
		if sub.name != "" {
			stringIndex = append(stringIndex, i)
		}
	}
	slices.SortFunc(stringIndex, func(a, b int) int { return cmp.Compare(cmds[a].name, cmds[b].name) })

	c.clookup = func(name string) int {
		pos, ok := slices.BinarySearchFunc(stringIndex, name, c.cmds.searchString)
		if ok {
			return stringIndex[pos]
		}
		return -1
	}

	return nil
}

type CmdOption interface {
	applyCommand(*Command) error
}

type cmdOptionFunc func(*Command) error

func (f cmdOptionFunc) applyCommand(cmd *Command) error {
	return f(cmd)
}

func Flags(flags ...Flag) CmdOption {
	return cmdOptionFunc(func(cmd *Command) error {
		return cmd.Flags(flags...)
	})
}

func Args(args ...Arg) CmdOption {
	return cmdOptionFunc(func(cmd *Command) error {
		return cmd.Args(args...)
	})
}

func Commands(cmds ...*Command) CmdOption {
	return cmdOptionFunc(func(cmd *Command) error {
		return cmd.Commands(cmds...)
	})
}

func Details(detail string) CmdOption {
	return cmdOptionFunc(func(cmd *Command) error {
		return cmd.Details(detail)
	})
}

func DetailsFor(detail string, opts ...Option) CmdOption {
	return cmdOptionFunc(func(cmd *Command) error {
		return cmd.DetailsFor(detail, opts...)
	})
}

func applyOpts(cmd *Command, opts []CmdOption) error {
	errs := make([]error, 0, len(opts))
	for _, opt := range opts {
		if err := opt.applyCommand(cmd); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
