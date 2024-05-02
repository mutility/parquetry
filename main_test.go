package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, map[string]func() int{
		"parquetry": func() int {
			if err := run(); err != nil {
				return 1
			}
			return 0
		},
	}))
}

func Test(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir: "testdata",
		Cmds: map[string]func(ts *testscript.TestScript, neg bool, args []string){
			"trim": func(ts *testscript.TestScript, neg bool, args []string) {
				remove := []byte{'\n'}
				if len(args) == 2 {
					remove = []byte(args[1])
				} else if len(args) != 1 {
					ts.Fatalf("usage: trim [chars=\n]")
				}

				data := []byte(ts.ReadFile(args[0]))

				// remove leading and all-but-one trailing
				for c, ok := bytes.CutPrefix(data, remove); ok; c, ok = bytes.CutPrefix(data, remove) {
					data = c
				}
				for c, ok := bytes.CutSuffix(data, remove); ok && bytes.HasSuffix(c, remove); c, ok = bytes.CutSuffix(data, remove) {
					data = c
				}
				ts.Stdout().Write(data)
			},
		},
		Setup: func(e *testscript.Env) error {
			pqs := filepath.Join("testdata", "parquet")
			tds, err := os.ReadDir(pqs)
			if err != nil {
				return err
			}
			for _, td := range tds {
				if strings.HasSuffix(td.Name(), ".parquet") {
					if err := os.Link(filepath.Join(pqs, td.Name()), filepath.Join(e.WorkDir, td.Name())); err != nil {
						return err
					}
				}
			}
			return nil
		},
	})
}
