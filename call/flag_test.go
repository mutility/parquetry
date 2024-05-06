package call

import (
	"slices"
	"testing"
)

func TestToggle(t *testing.T) {
	x := Toggle[bool](t.Name(), "")
	test0(t, x, "blah", true)
	test0(t, x, "blah", false)
	test0(t, x, "blah", true)
	test0(t, x, "blah", false)

	y := Toggle[bool](t.Name(), "").Default(true, "true")
	test0(t, y, "blah", false)
	test0(t, y, "blah", true)
	test0(t, y, "blah", false)
	test0(t, y, "blah", true)
}

func TestAdd(t *testing.T) {
	x := Add(2, t.Name(), "")
	test0(t, x, "blah", 2)
	test0(t, x, "blah", 4)
	test0(t, x, "blah", 6)
	test0(t, x, "blah", 8)

	y := Add(-2, t.Name(), "").Default(4, "4")
	test0(t, y, "blah", 2)
	test0(t, y, "blah", 0)
	test0(t, y, "blah", -2)
	test0(t, y, "blah", -4)

	r := Add(1.5, t.Name(), "")
	test0(t, r, "blah", 1.5)
	test0(t, r, "blah", 3.0)
	test0(t, r, "blah", 4.5)
	test0(t, r, "blah", 6.0)

	z := Add("z", t.Name(), "")
	test0(t, z, "blah", "z")
	test0(t, z, "blah", "zz")
	test0(t, z, "blah", "zzz")
	test0(t, z, "blah", "zzzz")
}

func TestOpt(t *testing.T) {
	x := Option[string](t.Name(), "")
	test1(t, x, "-f", "foo", "foo")
	test1(t, x, "-g", "bar", "bar")
	test1(t, x, "", "baz", "baz")
	test1(t, x, "", "quux", "quux")

	y := Enumerated(t.Name(), "", "foo", "bar", "baz")
	test1(t, y, "-f", "foo", "foo")
	test1(t, y, "-g", "bar", "bar")
	test1(t, y, "", "baz", "baz")
	test1err(t, y, "", "quux")

	z := Enumerated(t.Name(), "", 1, 3, 5)
	test1(t, z, "-f", "1", 1)
	test1(t, z, "-g", "3", 3)
	test1(t, z, "", "5", 5)
	test1err(t, z, "", "7")
}

func TestMulti(t *testing.T) {
	x := Multi[string](t.Name(), "")
	testN(t, x, []string{"x", "y"}, []string{"x", "y"})
	testN(t, x, []string{"x"}, []string{"x"})
	testNerr(t, x, []string{})
	testN(t, x, []string{"x", "y", "z"}, []string{"x", "y", "z"})
}

func test0[T comparable, Flag interface {
	Parse(string) (int, error)
	Value() T
}](t *testing.T, flag Flag, arg string, want T) {
	t.Helper()
	consumed, err := flag.Parse(arg)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != 0 {
		t.Error("consumed: got", consumed, "want 0")
	}
	if got := flag.Value(); got != want {
		t.Error("parsed: got", got, "want", want)
	}
}

func test1[T comparable, Flag interface {
	Parse(string, string) (int, error)
	Value() T
}](t *testing.T, flag Flag, arg1, arg2 string, want T) {
	t.Helper()
	consumed, err := flag.Parse(arg1, arg2)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != 1 {
		t.Error("consumed: got", consumed, "want 0")
	}
	if got := flag.Value(); got != want {
		t.Error("parsed: got", got, "want", want)
	}
}

func test1err[T comparable, Flag interface {
	Parse(string, string) (int, error)
	Value() T
}](t *testing.T, flag Flag, arg1, arg2 string) {
	t.Helper()
	_, err := flag.Parse(arg1, arg2)
	if err == nil {
		t.Fatal("want err")
	}
}

func testN[T comparable, Flag interface {
	Parse([]string) (int, error)
	Value() []T
}](t *testing.T, flag Flag, args []string, want []T) {
	t.Helper()
	consumed, err := flag.Parse(args)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(want) {
		t.Error("consumed: got", consumed, "want", len(want))
	}
	if got := flag.Value(); !slices.Equal(got, want) {
		t.Error("parsed: got", got, "want", want)
	}
}

func testNerr[T comparable, Flag interface {
	Parse([]string) (int, error)
	Value() []T
}](t *testing.T, flag Flag, args []string) {
	t.Helper()
	_, err := flag.Parse(args)
	if err == nil {
		t.Fatal("want err")
	}
}
