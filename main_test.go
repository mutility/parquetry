package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, map[string]func() int{
		"parquetry": func() int {
			if _, err := run(); err != nil {
				println(err.Error())
				return 1
			}
			return 0
		},
	}))
}

func Test(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir: "testdata",
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
