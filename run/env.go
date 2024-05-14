package run

import (
	"io"
	"os"
)

func DefaultEnviron() (env Environ) {
	env.fillDefaults()
	return env
}

type Environ struct {
	Args      []string
	Stdin     io.Reader
	Stdout    io.Writer
	Stderr    io.Writer
	Getenv    func(string) string
	LookupEnv func(string) (string, bool)
}

func (e *Environ) fillDefaults() {
	if e.Args == nil {
		e.Args = os.Args
	}
	if e.Stdin == nil {
		e.Stdin = os.Stdin
	}
	if e.Stdout == nil {
		e.Stdout = os.Stdout
	}
	if e.Stderr == nil {
		e.Stderr = os.Stderr
	}
	if e.Getenv == nil {
		e.Getenv = os.Getenv
	}
	if e.LookupEnv == nil {
		e.LookupEnv = os.LookupEnv
	}
}
