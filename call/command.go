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

func Program(name, description string, settings ...CommandSetting) *program {
	p := &program{
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
	for _, config := range settings {
		config(&p.command)
	}
	return p
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

type namedFlag struct {
	name string
	flag posConsumer
}

func (nf namedFlag) describe() string {
	desc := "<" + nf.name + ">"
	if annotate, ok := nf.flag.(interface{ describe(string) string }); ok {
		desc = annotate.describe(desc)
	}
	return desc
}

func (nf namedFlag) parsePosition(args []string) (int, error) {
	return nf.flag.parsePosition(nf.name, args)
}

type command struct {
	Name        string
	Description string
	Details     string
	stringFlags map[string]flagConsumer
	runeFlags   map[rune]flagConsumer
	Commands    []*command
	Positional  []namedFlag

	noHelp bool // suppress -h --help support
	runs   func(context.Context, Environ) error
}

type CommandSetting func(*command)

type flagConsumer interface {
	Flag
	parseFlag(string) (int, error)
}
type posConsumer interface {
	Flag
	parsePosition(string, []string) (int, error)
}

func ByName(flag flagConsumer, names ...string) CommandSetting {
	return func(c *command) {
		if len(names) == 0 {
			n, _ := flag.ID()
			names = []string{n}
		}
		for _, name := range names {
			c.addFlag(name, flag)
		}
	}
}

func ByOrder(flag posConsumer, names ...string) CommandSetting {
	return func(c *command) {
		switch len(names) {
		case 0:
			n, _ := flag.ID()
			c.addPos(flag, n)
		default:
			c.addPos(flag, names[0])
		}
	}
}

func (p *program) Command(name, description string, settings ...CommandSetting) *command {
	return addCommand(&p.Commands, name, description, settings)
}

func (p *program) Parse(args []string) (*command, error) {
	p.debug("parse args", args)
	if len(args) == 0 {
		return nil, MissingArgsError{p, &p.command, nil, nil}
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
			p.debug("  dashdash")
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
				p.debug("  flag", args[i:], "=>", pass, cut)

				consumed, err := 0, error(MissingArgsError{p, cur, nil, flag})
				if cut && len(pass) > 0 {
					consumed, err = flag.parseFlag(pass[0])
				} else if presence, ok := flag.(interface{ parseName() error }); ok {
					err = presence.parseName()
				} else if len(pass) > 0 {
					consumed, err = flag.parseFlag(pass[0])
				}
				if cut {
					consumed = 0
				}

				if err != nil {
					p.debug(" ", pass, err)
					return cur, err
				}
				p.debug("consumed flag", args[i:i+1+consumed], consumed, err)
				i += consumed
				continue
			}
			if !cur.noHelp && (arg == "-h" || arg == "--help") {
				showHelp = true
				continue
			}
			if !argAccepts(cur.nextArg().flag, arg) {
				return cur, UnexpectedArgError{args[i], p, cur}
			}
		}

		if pos := cur.nextArg(); pos.flag != nil {
			// look ahead and break on any unconsumable --flags
			pass := args[i:]
			for ai, av := range pass {
				if !argAccepts(pos.flag, av) {
					pass = pass[:ai]
					break
				}
			}
			if len(pass) == 0 {
				return cur, MissingArgsError{p: p, cmd: cur, named: []namedFlag{{pos.name, pos.flag}}}
			}
			consumed, err := pos.parsePosition(pass)
			p.debug("consumed args", strconv.Quote(pos.describe()), pass, consumed, err)
			if err != nil {
				return cur, err
			}
			i += consumed - 1

			continue
		}
		return cur, UnexpectedArgError{args[i], p, cur}
	}

	if showHelp {
		if cur.noHelp {
			return cur, ErrNoHelp
		}
		return helpCommand(p, cur), nil
	}

	if rem := cur.nextArgs(); rem != nil {
		return cur, MissingArgsError{p, cur, rem, nil}
	}
	return cur, nil
}

func argAccepts(f Flag, v string) bool {
	if f == nil {
		return false
	}

	if v == "-" || !strings.HasPrefix(v, "-") {
		return true
	}

	dash := f.dashes()
	switch {
	case strings.HasPrefix(v, "--"):
		return false // --xyz must be matched by flag, or -- protected)
	case dash&DashNumber != 0 && isNumeric(v):
		return true
	}
	return false
}

func (p *program) ReportError(err error) {
	if e, ok := err.(MissingArgsError); ok || errors.As(err, &e) {
		_ = writeUsage(p.Stdout, p, e.cmd)
		fmt.Fprintln(p.Stdout)
	} else if e, ok := err.(UnexpectedArgError); ok || errors.As(err, &e) {
		_ = writeUsage(p.Stdout, p, e.cmd)
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

type UnexpectedArgError struct {
	a   string
	p   *program
	cmd *command
}

func (e UnexpectedArgError) Error() string {
	if e.cmd != nil && e.cmd != &e.p.command {
		return e.cmd.Name + ": unexpected argument: " + strconv.Quote(e.a)
	}
	return "unexpected argument: " + strconv.Quote(e.a)
}

type MissingArgsError struct {
	p     *program
	cmd   *command
	named []namedFlag
	flag  Flag
}

func (e MissingArgsError) Error() string {
	argsHelp := make([]string, len(e.named))
	for i, f := range e.named {
		argsHelp[i] = f.describe()
	}
	if e.flag != nil {
		n, _ := e.flag.ID()
		argsHelp = append(argsHelp, n)
	}
	msg := `expected "` + strings.Join(argsHelp, " ") + `"`
	if e.cmd != nil && e.cmd != &e.p.command {
		msg = e.cmd.Name + ": " + msg
	}
	return msg
}

type ErrCmdExpected struct{ cmds []*command }

func (e ErrCmdExpected) Error() string {
	names := make([]string, len(e.cmds))
	for i, cmd := range e.cmds {
		names[i] = cmd.Name
	}
	return "expected one of: " + strings.Join(names, ", ")
}

type ErrNotInEnum[T any] struct {
	Name string
	Got  T
	Want []T
}

func (e ErrNotInEnum[T]) Error() string {
	return fmt.Sprintf("%s must be one of %v; got %v", e.Name, e.Want, e.Got)
}

func (c *command) Command(name, description string, settings ...CommandSetting) *command {
	return addCommand(&c.Commands, name, description, settings)
}

// Specify details for a command.
// Optionally specify flags that will refer to this command for its details.
func (c *command) Detail(help string, flags ...Flag) *command {
	c.Details = help
	for _, f := range flags {
		f.seeAlso(c)
	}
	return c
}

func (c *command) Runs(fn func(context.Context, Environ) error) *command {
	c.runs = fn
	return c
}

func addCommand(cmds *[]*command, name, description string, settings []CommandSetting) *command {
	cmd := &command{
		Name:        name,
		Description: description,
	}
	*cmds = append(*cmds, cmd)
	for _, config := range settings {
		config(cmd)
	}
	return cmd
}

func (c *command) addFlag(flags string, f flagConsumer) {
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

func (c *command) longFlag(name string, f flagConsumer) {
	if _, ok := c.stringFlags[name]; ok {
		panic(c.Name + ": multiple long definitions for " + name)
	}
	if c.stringFlags == nil {
		c.stringFlags = make(map[string]flagConsumer)
	}
	c.stringFlags[name] = f
}

func (c *command) shortFlag(name rune, f flagConsumer) {
	if _, ok := c.runeFlags[name]; ok {
		panic(c.Name + ": multiple short definitions for " + string(name))
	}
	if c.runeFlags == nil {
		c.runeFlags = make(map[rune]flagConsumer)
	}
	c.runeFlags[name] = f
}

func (c *command) addPos(a posConsumer, name string) {
	c.Positional = append(c.Positional, namedFlag{name, a})
}

func (c *command) matchCommand(arg string) *command {
	for _, cmd := range c.Commands {
		if cmd.Name == arg {
			return cmd
		}
	}
	return nil
}

func (c *command) nextArg() namedFlag {
	for _, arg := range c.Positional {
		if !arg.flag.assigned() {
			return arg
		}
	}
	return namedFlag{}
}

func (c *command) nextArgs() []namedFlag {
	for i, arg := range c.Positional {
		if !arg.flag.assigned() {
			return c.Positional[i:]
		}
	}
	return nil
}

func (c *command) reset() {
	for _, arg := range c.Positional {
		arg.flag.reset()
	}
	for _, cmd := range c.Commands {
		cmd.reset()
	}
}

func matchFlags(commands []*command, args []string) (flagConsumer, []string, bool) {
	for i := len(commands); i > 0; i-- {
		cmd := commands[i-1]
		if name, ok := strings.CutPrefix(args[0], "--"); ok {
			name, val, eql := strings.Cut(name, "=")
			if f, ok := cmd.stringFlags[name]; ok {
				if eql {
					return f, []string{val}, true
				}
				return f, args[1:], false
			}
		} else if name, ok := strings.CutPrefix(args[0], "-"); ok && args[0] != "-" {
			if f, ok := cmd.runeFlags[[]rune(name)[0]]; ok {
				return f, args[1:], false
			}
		}
	}
	return nil, nil, false
}

type Command interface {
	addFlag(string, flagConsumer)
	addPos(posConsumer, string)
}
