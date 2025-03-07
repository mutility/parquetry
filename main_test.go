package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mutility/cli/run"
	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"parquetry": func() {
			os.Exit(run.Main(runEnv))
		},
	})
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
				if _, err := ts.Stdout().Write(data); err != nil {
					ts.Fatalf("%v", err)
				}
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
