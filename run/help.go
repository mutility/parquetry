package run

import (
	"context"
	"fmt"
	"go/doc/comment"
	"io"
	"strings"
)

func helpCommand(a *Application, cmd *Command) *Command {
	return &Command{handler: func(ctx context.Context, env Environ) error {
		return cmd.PrintHelp(ctx, env, a)
	}}
}

func (c *Command) PrintHelp(ctx context.Context, env Environ, a *Application) error {
	writeUsage(env.Stdout, a, c)
	return nil
}

func writeUsage(w io.Writer, app *Application, cmd *Command) error {
	usage := []any{"Usage:", app.name}
	if cmd != &app.Command {
		usage = append(usage, cmd.CommandName())
	}
	if len(cmd.cmds) > 0 {
		usage = append(usage, "<command>")
	}
	if len(cmd.flags) > 0 || (cmd == &app.Command && len(cmd.cmds) > 0) {
		usage = append(usage, "[flags]")
	}
	for _, arg := range cmd.args {
		usage = append(usage, arg.describe())
	}
	fmt.Fprintln(w, usage...)

	if len(cmd.desc) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, cmd.desc)
	}
	if len(cmd.detail) > 0 {
		fmt.Fprintln(w)
		_, _ = w.Write((&comment.Printer{
			TextCodePrefix: "    ",
			TextWidth:      80,
		}).Text(new(comment.Parser).Parse(cmd.detail)))
	}

	if len(cmd.args) > 0 {
		args := makeTable("Arguments:")
		for _, arg := range cmd.args {
			args.Add(arg.describe(), arg.option.description())
		}
		args.Write(w)
	}

	// commands have help at least help flags, unless suppressed
	if len(cmd.flags) > 0 || !cmd.noHelp {
		anyRuneString := false
		for _, flag := range cmd.flags {
			if flag.rune != 0 && flag.string != "" {
				anyRuneString = true
			}
		}
		type flagInfo struct {
			flag Flag
			name string
			desc string
			rel  []string
		}
		info := make([]flagInfo, 0, len(cmd.flags)+1)
		info = append(info, flagInfo{Flag{}, "-h, --help", "Show context-sensitive help.", nil})
		for _, flag := range cmd.flags {
			var names []string
			if flag.rune != 0 {
				names = append(names, "-"+string(flag.rune))
			}
			if flag.string != "" {
				names = append(names, "--"+flag.string)
			}
			name := strings.Join(names, ", ")
			if (anyRuneString || !app.noHelp) && flag.rune == 0 {
				name = "    " + name
			}
			var see []string
			if flag.defaultSet {
				name += "=" + flag.defaultString
			} else if p := flag.hint; p != "" {
				name += "=" + p
			}
			for _, cmd := range flag.option.seeAlso() {
				see = append(see, fmt.Sprintf("(See %s %s --help)", app.name, cmd.name))
			}
			info = append(info, flagInfo{flag: flag, name: name, desc: flag.option.description(), rel: see})
		}

		flags := makeTable("Flags:")
		flags.Max = 22
		for _, flag := range info {
			flags.Add(flag.name, flag.desc)
			for _, see := range flag.rel {
				flags.Add("", see)
			}
		}
		flags.Write(w)
	}

	if len(cmd.cmds) > 0 {
		cmds := makeTable("Commands:")
		for _, cmd := range cmd.cmds {
			cmds.Add(cmd.name, cmd.desc)
		}
		cmds.Write(w)
		fmt.Fprintln(w, "\nRun \""+app.name+" <command> --help\" for more information on a command.")
	}

	_, err := w.Write(nil)
	return err
}

func makeTable(name string) table {
	return table{Name: name, Min: 6, Max: 12, Pad: 3}
}

type table struct {
	Name     string
	Items    [][2]string
	Min, Max int
	Pad      int
}

func (t *table) Add(col1, col2 string) { t.Items = append(t.Items, [2]string{col1, col2}) }

func (t *table) Write(w io.Writer) {
	longest := t.Min
	for _, it := range t.Items {
		longest = max(longest, len(it[0]))
	}
	if t.Max > 0 {
		longest = min(longest, t.Max)
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, t.Name)
	for _, it := range t.Items {
		if len(it[0]) > longest {
			fmt.Fprintf(w, "  %s\n  %-*s %s\n", it[0], longest+t.Pad, "", it[1])
		} else {
			fmt.Fprintf(w, "  %-*s %s\n", longest+t.Pad, it[0], it[1])
		}
	}
}
