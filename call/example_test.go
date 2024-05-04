package call_test

import (
	"fmt"
	"strings"

	"github.com/mutility/parquetry/call"
)

func ExampleProgram_var() {
	var toggle bool
	var increment int
	var decrement int
	var positive uint
	var negative int
	p := call.Program("program", "demonstrates call")
	call.ToggleVar(&toggle, "toggle", "Sets a toggle").FlagOn(p)
	call.OptionVar(&positive, "pos", "Sets a positive number").FlagOn(p)
	call.OptionVar(&negative, "neg", "Sets a negative number").FlagOn(p)
	call.AddVar(&increment, 1, "u", "Increment up").FlagsOn("-u", p)
	call.AddVar(&decrement, -1, "d", "Decrement down").FlagsOn("-d", p)

	cmd, err := p.Parse(strings.Fields("prog --toggle --pos 3 --neg -3 -u -u -d -d"))

	fmt.Println(cmd.Name, err, toggle, positive, negative, increment, decrement)
	// output: program <nil> true 3 -3 2 -2
}

func ExampleProgram_out() {
	p := call.Program("program", "demonstrates call")
	toggle := call.Toggle[bool]("toggle", "Sets a toggle").FlagOn(p)
	positive := call.Option[int]("pos", "Sets a positive number").FlagOn(p)
	negative := call.Option[int]("neg", "Sets a negative number").FlagOn(p)
	increment := call.Add(1, "u", "Increment up").FlagsOn("-u", p)
	decrement := call.Add(-1, "d", "Decrement down").FlagsOn("-d", p)

	cmd, err := p.Parse(strings.Fields("prog --toggle --pos 3 --neg -3 -u -u -d -d"))

	fmt.Println(cmd.Name, err, toggle.Value(), positive.Value(), negative.Value(), increment.Value(), decrement.Value())
	// output: program <nil> true 3 -3 2 -2
}

func ExampleProgram_command() {
	var a, b, c string
	p := call.Program("program", "demonstrates commands")
	call.OptionVar(&a, "a", "Sets a").FlagOn(p)
	call.OptionVar(&b, "b", "Sets b").FlagOn(p)

	cmd := p.Command("cmd", "a command")
	call.OptionVar(&c, "c", "Sets c").FlagOn(cmd)

	cmd, err := p.Parse(strings.Fields("prog --a A cmd --b B --c C"))
	fmt.Println(cmd.Name, err, a, b, c)

	cmd, err = p.Parse(strings.Fields("prog --a D --b E --c F"))
	fmt.Println(cmd.Name, err, a, b, c)

	// output:
	// cmd <nil> A B C
	// program unexpected: "--c" D E C
}

func ExampleProgram_subcommand() {
	var a, b, c, d string
	p := call.Program("program", "demonstrates subcommands")
	call.OptionVar(&a, "a", "Sets a").FlagOn(p)
	call.OptionVar(&b, "b", "Sets b").FlagOn(p)

	cmd := p.Command("cmd", "a command")
	call.OptionVar(&c, "c", "Sets c").FlagOn(cmd)

	sub := cmd.Command("sub", "a command")
	call.OptionVar(&d, "d", "Sets d").FlagOn(sub)

	cmd, err := p.Parse(strings.Fields("prog --a A cmd --b B --c C"))
	fmt.Println(cmd.Name, err, []string{a, b, c, d})

	cmd, err = p.Parse(strings.Fields("prog --a A cmd --b B sub --c C --d D"))
	fmt.Println(cmd.Name, err, []string{a, b, c, d})

	// output:
	// cmd <nil> [A B C ]
	// sub <nil> [A B C D]
}

func ExampleEnumerated_var() {
	p := call.Program("program", "demonstrates call")
	c := call.Enumerated("color", "of the rainbow",
		"red", "orange", "yellow", "green", "blue", "violet").FlagOn(p)

	cmd, err := p.Parse(strings.Fields("prog --color yellow"))
	fmt.Println("1:", cmd.Name, err, c.Value())
	cmd, err = p.Parse(strings.Fields("prog --color brown"))
	fmt.Println("2:", cmd.Name, err, c.Value())

	// output:
	// 1: program <nil> yellow
	// 2: program color must be one of [red orange yellow green blue violet]; got brown yellow
}

func ExampleSingle_root() {
	p := call.Program("prog", "demonstrates a positional argument on the program")
	v := call.Option[string]("v", "accepts one value").PosOn(p)

	cmd, err := p.Parse(strings.Fields("prog"))
	fmt.Println("0:", cmd.Name, err, []string{v.Value()})
	cmd, err = p.Parse(strings.Fields("prog arg"))
	fmt.Println("1:", cmd.Name, err, []string{v.Value()})
	p.Reset()
	cmd, err = p.Parse(strings.Fields("prog argh argh"))
	fmt.Println("2:", cmd.Name, err, []string{v.Value()})

	// output:
	// 0: prog expected: <v> []
	// 1: prog <nil> [arg]
	// 2: prog unexpected: "argh" [argh]
}

func ExampleSingle_command() {
	p := call.Program("prog", "demonstrates a positional argument on the program")
	c := p.Command("cmd", "a command")
	v := call.Multi[string]("v", "accepts multiple values").PosOn(c)

	cmd, err := p.Parse(strings.Fields("prog"))
	fmt.Println("0:", cmd.Name, err, v.Value())
	cmd, err = p.Parse(strings.Fields("prog cmd"))
	fmt.Println("1:", cmd.Name, err, v.Value())
	cmd, err = p.Parse(strings.Fields("prog cmd arg"))
	fmt.Println("2:", cmd.Name, err, v.Value())
	p.Reset()
	cmd, err = p.Parse(strings.Fields("prog cmd arg1 arg2"))
	fmt.Println("3:", cmd.Name, err, v.Value())

	// output:
	// 0: prog <nil> []
	// 1: cmd expected: <v> []
	// 2: cmd <nil> [arg]
	// 3: cmd <nil> [arg1 arg2]
}
