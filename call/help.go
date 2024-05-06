package call

import (
	"cmp"
	"context"
	"fmt"
	"go/doc"
	"io"
	"slices"
	"strings"
)

func helpCommand(p *program, cmd *command) *command {
	return &command{runs: func(ctx context.Context, e Environ) error {
		return runHelpCommand(ctx, e, p, cmd)
	}}
}

func runHelpCommand(ctx context.Context, e Environ, p *program, cmd *command) error {
	return writeUsage(e.Stdout, p, cmd)
}

func writeUsage(w io.Writer, p *program, cmd *command) error {
	usage := []any{"Usage:", p.Name}
	if cmd != &p.command {
		usage = append(usage, cmd.Name)
	}
	if len(cmd.Commands) > 0 {
		usage = append(usage, "<command>")
	}
	if len(cmd.stringFlags)+len(cmd.runeFlags) > 0 || (cmd == &p.command && len(cmd.Commands) > 0) {
		usage = append(usage, "[flags]")
	}
	for _, pos := range cmd.Positional {
		usage = append(usage, helpDescribe(pos))
	}
	fmt.Fprintln(w, usage...)

	if len(cmd.Description) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, cmd.Description)
	}
	if len(cmd.Details) > 0 {
		fmt.Fprintln(w)
		doc.ToText(w, strings.TrimSpace(cmd.Details), "", "    ", 80)
	}

	if len(cmd.Positional) > 0 {
		args := makeTable("Arguments:")
		for _, pos := range cmd.Positional {
			n, desc := pos.ID()
			n = "<" + cmp.Or(pos.PosName(), n) + ">"
			if _, ok := pos.(ManyParser); ok {
				n += " ..."
			}
			args.Add(n, desc)
		}
		args.Write(w)
	}

	// commands have help at least help flags, unless suppressed
	if len(cmd.stringFlags)+len(cmd.runeFlags) > 0 || !cmd.noHelp {
		type names struct {
			runes   []rune
			strings []string
		}
		cmdFlags := map[Flag]names{nil: {runes: []rune{'h'}, strings: []string{"help"}}}
		for r, flag := range cmd.runeFlags {
			n := cmdFlags[flag]
			n.runes = append(n.runes, r)
			cmdFlags[flag] = n
		}
		for s, flag := range cmd.stringFlags {
			n := cmdFlags[flag]
			n.strings = append(n.strings, s)
			cmdFlags[flag] = n
		}
		type flagInfo struct {
			flag Flag
			name string
			desc string
			sort string
			rel  []string
		}
		info := make([]flagInfo, 0, len(cmdFlags))
		for f, n := range cmdFlags {
			var names []string
			for _, r := range n.runes {
				names = append(names, "-"+string(r))
				break // only show one short flag
			}
			for _, s := range n.strings {
				names = append(names, "--"+s)
				break // only show one long flag
			}
			sort, desc := "_", "Show context-sensitive help."
			if f != nil {
				sort, desc = f.ID()
			}
			name := strings.Join(names, ", ")
			if (len(cmd.runeFlags) > 0 || !p.noHelp) && len(n.runes) == 0 {
				name = "    " + name
			}
			var see []string
			if f != nil {
				if d := f.DefaultString(); d != "" {
					name += "=" + d
				} else if p := f.Placeholder(); p != "" {
					name += "=" + p
				}
				for _, cmd := range f.See() {
					see = append(see, fmt.Sprintf("(See %s %s --help)", p.Name, cmd.Name))
				}
			}
			info = append(info, flagInfo{flag: f, name: name, desc: desc, sort: sort, rel: see})
		}
		slices.SortFunc(info, func(a, b flagInfo) int { return cmp.Compare(a.sort, b.sort) })

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

	if len(cmd.Commands) > 0 {
		cmds := makeTable("Commands:")
		for _, cmd := range cmd.Commands {
			cmds.Add(cmd.Name, cmd.Description)
		}
		cmds.Write(w)
		fmt.Fprintln(w, "\nRun \""+p.Name+" <command> --help\" for more information on a command.")
	}

	return nil
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

func helpDescribe(pos Flag) string {
	n, _ := pos.ID()
	desc := "<" + cmp.Or(pos.PosName(), n) + ">"
	if pos.consumes() == ConsumesSlice {
		desc += " ..."
	}
	return desc
}
