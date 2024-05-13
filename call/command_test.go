package call_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mutility/parquetry/call"
)

func TestProgram_Cat(t *testing.T) {
	for _, tt := range []struct {
		Args   []string
		Error  func(*testing.T, error)
		Files  int
		Number bool
	}{
		{Args: []string{"None"}, Error: wantMissing("")},
		{Args: []string{"One", "-"}, Files: 1},
		{Args: []string{"Two", "-", "-"}, Files: 2},
		{Args: []string{"ZOne", "-z", "-"}, Error: wantUnexpected("")},
		{Args: []string{"OneZ", "-", "-z"}, Error: wantUnexpected("")},
		{Args: []string{"OneStop", "-", "--"}, Files: 1},
		{Args: []string{"OneStopOne", "-", "--", "-"}, Error: wantUnexpected("")},
		{Args: []string{"Num", "-n"}, Error: wantMissing(t.Name() + "/Num")},
		{Args: []string{"NumOne", "-n", "-"}, Files: 1, Number: true},
		{Args: []string{"NumTwo", "-n", "-", "-"}, Files: 2, Number: true},
		{Args: []string{"NumOneStop", "-n", "-", "--"}, Files: 1, Number: true},
		{Args: []string{"NumOneStopOne", "-n", "-", "--", "-"}, Error: wantUnexpected("")},
	} {
		t.Run(tt.Args[0], func(t *testing.T) {
			ran := false
			number := call.Toggle[bool]("number", "Output line numbers")
			files := call.Multi[string]("files", "File to output")
			p := call.Program(
				t.Name(), "tests "+t.Name(),
				call.ByName(number, "-n", "--number"),
				call.ByOrder(files),
			)
			p.Runs(func(ctx context.Context, e call.Environ) error {
				ran = true
				return nil
			})

			cmd, err := p.Parse(tt.Args)
			if tt.Error != nil {
				tt.Error(t, err)
			} else if err != nil {
				t.Fatalf("error (%T): %[1]v", err)
			} else {
				_ = p.RunCommand(context.Background(), cmd)
				if got := number.Value(); got != tt.Number {
					t.Error("number: got", got, "want", tt.Number)
				}
				if got := files.Value(); len(got) != tt.Files {
					t.Error("files: got", len(got), "want", tt.Files, got)
				}
				if tt.Error == nil != ran {
					t.Error("ran: got", ran, "want", tt.Error == nil)
				}
			}
		})
	}
}

func wantMissing(flag string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		var got call.MissingArgsError
		if !errors.As(err, &got) {
			t.Errorf("error type: got %T want %T", err, got)
		} else {
			name, _, ok := strings.Cut(got.Error(), ":")
			if flag != "" && ok && flag != name {
				t.Errorf("error flag: got %v want %v (%v)", name, flag, err)
			}
		}
	}
}

func wantUnexpected(cmd string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		var got call.UnexpectedArgError
		if !errors.As(err, &got) {
			t.Errorf("error type: got %T want %T", err, got)
		} else {
			name, _, ok := strings.Cut(got.Error(), ":")
			if cmd != "" && ok && cmd != name {
				t.Errorf("error flag: got %v want %v", name, cmd)
			}
		}
	}
}
