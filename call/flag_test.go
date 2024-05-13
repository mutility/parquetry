package call

import (
	"cmp"
	"slices"
	"testing"
)

func TestToggle(t *testing.T) {
	x := Toggle[bool](t.Name(), "")
	testFlag0(t, x, "blah", true)
	testFlag0(t, x, "blah", false)
	testFlag0(t, x, "blah", true)
	testFlag0(t, x, "blah", false)

	y := Toggle[bool](t.Name(), "", Default(true))
	testFlag0(t, y, "blah", false)
	testFlag0(t, y, "blah", true)
	testFlag0(t, y, "blah", false)
	testFlag0(t, y, "blah", true)
}

func TestAdd(t *testing.T) {
	x := Add(2, t.Name(), "")
	testFlag0(t, x, "blah", 2)
	testFlag0(t, x, "blah", 4)
	testFlag0(t, x, "blah", 6)
	testFlag0(t, x, "blah", 8)

	y := Add(-2, t.Name(), "", Default(4))
	testFlag0(t, y, "blah", 2)
	testFlag0(t, y, "blah", 0)
	testFlag0(t, y, "blah", -2)
	testFlag0(t, y, "blah", -4)

	r := Add(1.5, t.Name(), "")
	testFlag0(t, r, "blah", 1.5)
	testFlag0(t, r, "blah", 3.0)
	testFlag0(t, r, "blah", 4.5)
	testFlag0(t, r, "blah", 6.0)

	z := Add("z", t.Name(), "")
	testFlag0(t, z, "blah", "z")
	testFlag0(t, z, "blah", "zz")
	testFlag0(t, z, "blah", "zzz")
	testFlag0(t, z, "blah", "zzzz")
}

func TestOpt(t *testing.T) {
	x := Option[string](t.Name(), "")
	testFlag(t, x, "foo", "foo")
	testFlag(t, x, "bar", "bar")
	testPos1(t, x, []string{"baz", "quux"}, "baz")
	testPos1(t, x, []string{"quux"}, "quux")

	y := Option[string](t.Name(), "").EnumRaw("foo", "bar", "baz")
	testFlag(t, y, "foo", "foo")
	testFlag(t, y, "bar", "bar")
	testPos1(t, y, []string{"baz", "quux"}, "baz")
	testFlagerr(t, y, "quux")
	testPoserr(t, y, []string{"quux", "x"})

	z := Option[int](t.Name(), "").EnumFunc(cmp.Compare[int], 1, 3, 5)
	testFlag(t, z, "1", 1)
	testFlag(t, z, "3", 3)
	testPos1(t, z, []string{"5"}, 5)
	testFlagerr(t, z, "7")
	testPoserr(t, z, []string{"7", "x"})

	z2 := Option[int](t.Name(), "").EnumMap(map[string]int{"a": 1, "b": 3, "c": 5})
	testFlag(t, z2, "a", 1)
	testFlag(t, z2, "b", 3)
	testPos1(t, z2, []string{"c"}, 5)
	testFlagerr(t, z2, "d")
	testPoserr(t, z2, []string{"d"})
}

func TestMulti(t *testing.T) {
	x := Multi[string](t.Name(), "")
	testPosN(t, x, []string{"x", "y"}, []string{"x", "y"})
	testPosN(t, x, []string{"x"}, []string{"x"})
	testPoserr(t, x, []string{})
	testPosN(t, x, []string{"x", "y", "z"}, []string{"x", "y", "z"})
}

func testFlag0[T comparable, Flag interface {
	flagConsumer
	Value() T
}](t *testing.T, flag Flag, arg string, want T) {
	t.Helper()
	consumed, err := flag.parseFlag(arg)
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

func testFlag[T comparable, Flag interface {
	flagConsumer
	Value() T
}](t *testing.T, flag Flag, arg string, want T) {
	t.Helper()
	consumed, err := flag.parseFlag(arg)
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

func testFlagerr[T comparable, Flag interface {
	parseFlag(string) (int, error)
	Value() T
}](t *testing.T, flag Flag, arg1 string) {
	t.Helper()
	_, err := flag.parseFlag(arg1)
	if err == nil {
		t.Fatal("want err")
	}
}

func testPos1[T comparable, Flag interface {
	parsePosition(string, []string) (int, error)
	Value() T
}](t *testing.T, flag Flag, args []string, want T) {
	t.Helper()
	consumed, err := flag.parsePosition("...", args)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != 1 {
		t.Error("consumed: got", consumed, "want", 1)
	}
	if got := flag.Value(); got != want {
		t.Error("parsed: got", got, "want", want)
	}
}

func testPosN[T comparable, Flag interface {
	parsePosition(string, []string) (int, error)
	Value() []T
}](t *testing.T, flag Flag, args []string, want []T) {
	t.Helper()
	consumed, err := flag.parsePosition("...", args)
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

func testPoserr[Flag posConsumer](t *testing.T, flag Flag, args []string) {
	t.Helper()
	_, err := flag.parsePosition("...", args)
	if err == nil {
		t.Fatal("want err")
	}
}
