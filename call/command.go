package call

import (
	"context"
	"errors"
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
	Details     string
	stringFlags map[string]Flag
	runeFlags   map[rune]Flag
	Commands    []*command
	Positional  []Flag

	noHelp bool // suppress -h --help support
	runs   func(context.Context, Environ) error
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
	showHelp := false
	cur := &p.command
	context := []*command{cur}

	for i := 1; i < len(args); i++ {
		arg := args[i]
		p.debug("rem args", args[i:])
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
			if flag, pass, cut := matchFlags(context, args[i:]); flag != nil {
				p.debug("  flag", args[i:], "=>", pass)

				consumed, err := 0, ErrMissingArgument
				switch flag := flag.(type) {
				case ManyParser:
					consumed, err = flag.Parse(pass)
				case SingleParser:
					if len(pass) > 1 {
						consumed, err = flag.Parse(pass[0], pass[1])
					}
				case PresenceParser:
					consumed, err = flag.Parse(pass[0])
				}
				if cut {
					consumed = 0
				}

				if err != nil {
					p.debug(" ", pass, err)
					return cur, err
				}
				p.debug("consumed flag", args[i:i+1+consumed])
				i += consumed
				continue
			}
			if !cur.noHelp && (arg == "-h" || arg == "--help") {
				showHelp = true
				continue
			}
			if !argAccepts(cur.nextArg(), arg) {
				return cur, ErrArgUnexpected{args[i], cur}
			}
		}

		if pos := cur.nextArg(); pos != nil {
			var consumed int
			var err error
			switch parser := pos.(type) {
			case ManyParser:
				// look ahead and break on any unconsumable --flags
				pass := args[i:]
				for ai, av := range pass[1:] {
					if !argAccepts(pos, av) {
						pass = pass[:ai+1]
						break
					}
				}
				consumed, err = parser.Parse(pass)
				p.debug("consumed args (many)", helpDescribe(pos), pass, consumed, err)
			case SingleParser:
				consumed, err = parser.Parse("", args[i])
				p.debug("consumed arg (single)", pos, []string{"", args[i]}, consumed)
			}
			if err != nil {
				return cur, err
			}
			i += consumed - 1

			continue
		}
		return cur, ErrArgUnexpected{args[i], cur}
	}

	if showHelp {
		if cur.noHelp {
			return cur, ErrNoHelp
		}
		return helpCommand(p, cur), nil
	}

	if rem := cur.nextArgs(); rem != nil {
		return cur, ErrArgsExpected{rem, cur}
	}
	return cur, nil
}

func argAccepts(f Flag, v string) bool {
	if f == nil {
		return false
	}

	if !strings.HasPrefix(v, "-") {
		return true
	}

	dash := f.dashes()
	switch {
	case strings.HasPrefix(v, "--"):
		return false // --xyz must be matched by flag, or -- protected)
	case dash&DashStdio != 0 && v == "-":
		return true
	case dash&DashNumber != 0 && isNumeric(v):
		return true
	}
	return false
}

func (p *program) ReportError(err error) {
	if e, ok := err.(ErrArgsExpected); ok || errors.As(err, &e) {
		writeUsage(p.Stdout, p, e.command)
		fmt.Fprintln(p.Stdout)
	} else if e, ok := err.(ErrArgUnexpected); ok || errors.As(err, &e) {
		writeUsage(p.Stdout, p, e.command)
		fmt.Fprintln(p.Stdout)
	}
	fmt.Fprintln(p.Stderr, p.Name+":", "error:", err)
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

var ErrNoHelp = fmt.Errorf("help unavailable")

type ErrArgUnexpected struct {
	a       string
	command *command
}

func (e ErrArgUnexpected) Error() string { return "unexpected argument: " + strconv.Quote(e.a) }

type ErrArgsExpected struct {
	a       []Flag
	command *command
}

func (e ErrArgsExpected) Error() string {
	argsHelp := make([]string, len(e.a))
	for i, f := range e.a {
		argsHelp[i] = helpDescribe(f)
	}
	return `expected "` + strings.Join(argsHelp, " ") + `"`
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

func (c *command) Detail(help string) *command {
	c.Details = help
	return c
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
	switch f.(type) {
	case ManyParser, SingleParser, PresenceParser:
	default:
		panic(fmt.Sprintf("unsupported flag parser: %T", f))
	}
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
	switch a.(type) {
	case ManyParser, SingleParser:
	default:
		panic("unsupported positional parser")
	}
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

func (c *command) nextArgs() []Flag {
	for i, arg := range c.Positional {
		if !arg.assigned() {
			return c.Positional[i:]
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

func matchFlags(commands []*command, args []string) (Flag, []string, bool) {
	for i := len(commands); i > 0; i-- {
		cmd := commands[i-1]
		if name, ok := strings.CutPrefix(args[0], "--"); ok {
			name, val, eql := strings.Cut(name, "=")
			if f, ok := cmd.stringFlags[name]; ok {
				if eql {
					return f, []string{name, val}, true
				}
				return f, args, false
			}
		} else if name, ok := strings.CutPrefix(args[0], "-"); ok && args[0] != "-" {
			if f, ok := cmd.runeFlags[[]rune(name)[0]]; ok {
				return f, args, false
			}
		}
	}
	return nil, nil, false
}

type Command interface {
	addFlag(string, Flag)
	addPos(Flag)
}
