package call

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func Program(name, description string) *program {
	return &program{
		command: command{
			Name:        name,
			Description: description,
		},
		Environ: Environ{
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		},
	}
}

type program struct {
	Environ
	command
	argv0 string
}

type Environ struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

type command struct {
	Name        string
	Description string
	stringFlags map[string]Flag
	runeFlags   map[rune]Flag
	Commands    []*command
	Positional  []Flag

	runs func(context.Context, Environ) error
}

func (p *program) Command(name, description string) *command {
	return addCommand(&p.Commands, name, description)
}

func (p *program) Parse(args []string) (*command, error) {
	p.debug("parse args", args)
	if len(args) == 0 {
		return nil, ErrMissingArgument
	}
	p.argv0 = args[0]

	canFlag := true
	cur := &p.command
	context := []*command{cur}

	for i := 1; i < len(args); i++ {
		arg := args[i]
		p.debug("parse arg", arg)
		if arg == "--" {
			canFlag = false
			continue
		}

		if cmd := cur.matchCommand(arg); cmd != nil {
			cur = cmd
			p.debug("consumed command", arg)
			context = append(context, cmd)
			continue
		}

		if canFlag && strings.HasPrefix(arg, "-") {
			if flag, eqlArgs := matchFlags(context, arg); flag != nil {
				p.debug("with eql", args[1+1:], eqlArgs)
				consumed, err := p.parseFlagWithOverride(flag, args[i+1:], eqlArgs)
				if err != nil {
					return cur, err
				}
				p.debug("consumed flag", args[i:i+1+consumed])
				i += consumed
				continue
			}
			type dashAccepter interface {
				acceptDash() bool
			}
			if pos, ok := cur.nextArg().(dashAccepter); !ok || strings.HasPrefix(arg, "--") || !pos.acceptDash() {
				return cur, ErrArgument(args[i])
			}
		}

		if pos := cur.nextArg(); pos != nil {
			consumed, err := pos.Parse(args[i:])
			if err != nil {
				return cur, err
			}
			p.debug("consumed arg", args[i:i+consumed])
			i += consumed - 1
			continue
		}
		return cur, ErrArgument(args[i])
	}

	if arg := cur.nextArg(); arg != nil {
		return cur, ErrArgExpected{arg}
	}

	return cur, nil
}

func (p *program) parseFlagWithOverride(flag Flag, args, override []string) (consumed int, err error) {
	if len(override) > 0 {
		_, err := flag.Parse(override)
		return 0, err // cannot consume past override
	}
	return flag.Parse(args)
}

func (p *program) ReportError(err error) {
	fmt.Fprintln(p.Stderr, p.Name+":", err)
}

func (p *program) debug(a ...any) {
	// fmt.Fprintln(p.Stderr, append([]any{p.Name + ":"}, a...)...)
}

func (p *program) Reset() {
	p.command.reset()
}

func (p *program) RunCommand(ctx context.Context, cmd *command) error {
	if cmd.runs == nil {
		if len(cmd.Commands) > 0 {
			return ErrCmdExpected{cmd.Commands}
		}
		return nil
	}
	return cmd.runs(ctx, p.Environ)
}

type ErrArgument string

func (e ErrArgument) Error() string { return "unexpected: " + strconv.Quote(string(e)) }

type ErrArgExpected struct{ a Flag }

func (e ErrArgExpected) Error() string {
	n, _ := e.a.ID()
	return "expected: <" + n + ">"
}

type ErrCmdExpected struct{ cmds []*command }

func (e ErrCmdExpected) Error() string {
	names := make([]string, len(e.cmds))
	for i, cmd := range e.cmds {
		names[i] = cmd.Name
	}
	return "expected one of: " + strings.Join(names, ", ")
}

type ErrNotInEnum[T comparable] struct {
	Name string
	Got  T
	Want []T
}

func (e ErrNotInEnum[T]) Error() string {
	return fmt.Sprintf("%s must be one of %v; got %v", e.Name, e.Want, e.Got)
}

func (c *command) Command(name, description string) *command {
	return addCommand(&c.Commands, name, description)
}

func (c *command) Runs(fn func(context.Context, Environ) error) *command {
	c.runs = fn
	return c
}

func addCommand(cmds *[]*command, name, description string) *command {
	cmd := &command{
		Name:        name,
		Description: description,
	}
	*cmds = append(*cmds, cmd)
	return cmd
}

func (c *command) addFlag(flags string, f Flag) {
	if len(flags) == 0 {
		name, _ := f.ID()
		c.longFlag(name, f)
	}
	for _, name := range strings.Split(flags, "|") {
		if name, long := strings.CutPrefix(name, "--"); long {
			c.longFlag(name, f)
		} else if name, short := strings.CutPrefix(name, "-"); short {
			for _, name := range name {
				c.shortFlag(name, f)
			}
		} else if len(name) > 1 {
			c.longFlag(name, f)
		} else {
			for _, name := range name {
				c.shortFlag(name, f)
			}
		}
	}
}

func (c *command) longFlag(name string, f Flag) {
	if _, ok := c.stringFlags[name]; ok {
		panic(c.Name + ": multiple long definitions for " + name)
	}
	if c.stringFlags == nil {
		c.stringFlags = make(map[string]Flag)
	}
	c.stringFlags[name] = f
}

func (c *command) shortFlag(name rune, f Flag) {
	if _, ok := c.runeFlags[name]; ok {
		panic(c.Name + ": multiple short definitions for " + string(name))
	}
	if c.runeFlags == nil {
		c.runeFlags = make(map[rune]Flag)
	}
	c.runeFlags[name] = f
}

func (c *command) addPos(a Flag) {
	c.Positional = append(c.Positional, a)
}

func (c *command) matchCommand(arg string) *command {
	for _, cmd := range c.Commands {
		if cmd.Name == arg {
			return cmd
		}
	}
	return nil
}

func (c *command) nextArg() Flag {
	for _, arg := range c.Positional {
		if !arg.assigned() {
			return arg
		}
	}
	return nil
}

func (c *command) reset() {
	for _, arg := range c.Positional {
		arg.reset()
	}
	for _, cmd := range c.Commands {
		cmd.reset()
	}
}

func matchFlags(commands []*command, arg string) (Flag, []string) {
	for i := len(commands); i > 0; i-- {
		cmd := commands[i-1]
		if name, ok := strings.CutPrefix(arg, "--"); ok {
			name, val, eql := strings.Cut(name, "=")
			if f, ok := cmd.stringFlags[name]; ok {
				if eql {
					return f, []string{val}
				}
				return f, nil
			}
		} else if name, ok := strings.CutPrefix(arg, "-"); ok {
			if f, ok := cmd.runeFlags[[]rune(name)[0]]; ok {
				return f, nil
			}
		}
	}
	return nil, nil
}

type Command interface {
	addFlag(string, Flag)
	addPos(Flag)
}
