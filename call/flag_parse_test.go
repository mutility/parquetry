package call

import (
	"fmt"
	"testing"
)

func TestParser(t *testing.T) {
	test := func(name string, parser any, want any) {
		got, want := fmt.Sprintf("%p", parser), fmt.Sprintf("%p", want)
		if got != want {
			t.Error(name, "parser: got", parser, "want", want)
		}
	}

	test("string", parserFor[string](), parseString[string])

	test("bool", parserFor[bool](), parseBool[bool])

	test("int", parserFor[int](), parseInt[int])
	test("int8", parserFor[int8](), parseInt[int8])
	test("int16", parserFor[int16](), parseInt[int16])
	test("int32", parserFor[int32](), parseInt[int32])
	test("int64", parserFor[int64](), parseInt[int64])

	test("uint", parserFor[uint](), parseUint[uint])
	test("uint8", parserFor[uint8](), parseUint[uint8])
	test("uint16", parserFor[uint16](), parseUint[uint16])
	test("uint32", parserFor[uint32](), parseUint[uint32])
	test("uint64", parserFor[uint64](), parseUint[uint64])

	test("float32", parserFor[float32](), parseFloat[float32])
	test("float64", parserFor[float64](), parseFloat[float64])
}

func TestTypedParser(t *testing.T) {
	type notString string
	testStringParser(t, "notUint", "123", notString("123"))

	type notBool bool
	testBoolParser(t, "notBool", "true", notBool(true))

	type notInt int
	type notInt8 int8
	type notInt16 int16
	type notInt32 int32
	type notInt64 int64
	testIntParser(t, "notInt", "123", notInt(123))
	testIntParser(t, "notInt8", "123", notInt8(123))
	testIntParser(t, "notInt16", "123", notInt16(123))
	testIntParser(t, "notInt32", "123", notInt32(123))
	testIntParser(t, "notInt64", "123", notInt64(123))

	type notUint uint
	type notUint8 uint8
	type notUint16 uint16
	type notUint32 uint32
	type notUint64 uint64
	testUintParser(t, "notUint", "123", notUint(123))
	testUintParser(t, "notUint8", "123", notUint8(123))
	testUintParser(t, "notUint16", "123", notUint16(123))
	testUintParser(t, "notUint32", "123", notUint32(123))
	testUintParser(t, "notUint64", "123", notUint64(123))

	type notFloat32 float32
	type notFloat64 float64
	testFloatParser(t, "notFloat32", "123", notFloat32(123))
	testFloatParser(t, "notFloat64", "123", notFloat64(123))
}

func testStringParser[T ~string](t *testing.T, name, arg string, want T) {
	if parserFor[T]() != nil {
		t.Errorf("%s: got parser for %T", name, want)
	}
	p := StringOption[T](name, "").parser
	if p == nil {
		t.Errorf("%s: no int parser for %T", name, want)
	}
	if got, err := p(arg); err != nil {
		t.Errorf("%s: parse error: %v", name, err)
	} else if got != want {
		t.Errorf("%s: parse(%s): got %#v want %#v", name, arg, got, want)
	}
}

func testBoolParser[T ~bool](t *testing.T, name, arg string, want T) {
	if parserFor[T]() != nil {
		t.Errorf("%s: got parser for %T", name, want)
	}
	p := BoolOption[T](name, "").parser
	if p == nil {
		t.Errorf("%s: no int parser for %T", name, want)
	}
	if got, err := p(arg); err != nil {
		t.Errorf("%s: parse error: %v", name, err)
	} else if got != want {
		t.Errorf("%s: parse(%s): got %#v want %#v", name, arg, got, want)
	}
}

func testIntParser[T ~int | ~int8 | ~int16 | ~int32 | ~int64](t *testing.T, name, arg string, want T) {
	if parserFor[T]() != nil {
		t.Errorf("%s: got parser for %T", name, want)
	}
	p := IntOption[T](name, "").parser
	if p == nil {
		t.Errorf("%s: no int parser for %T", name, want)
	}
	if got, err := p(arg); err != nil {
		t.Errorf("%s: parse error: %v", name, err)
	} else if got != want {
		t.Errorf("%s: parse(%s): got %#v want %#v", name, arg, got, want)
	}
}

func testUintParser[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](t *testing.T, name, arg string, want T) {
	if parserFor[T]() != nil {
		t.Errorf("%s: got parser for %T", name, want)
	}
	p := UintOption[T](name, "").parser
	if p == nil {
		t.Errorf("%s: no uint parser for %T", name, want)
	}
	if got, err := p(arg); err != nil {
		t.Errorf("%s: parse error: %v", name, err)
	} else if got != want {
		t.Errorf("%s: parse(%s): got %#v want %#v", name, arg, got, want)
	}
}

func testFloatParser[T ~float32 | ~float64](t *testing.T, name, arg string, want T) {
	if parserFor[T]() != nil {
		t.Errorf("%s: got parser for %T", name, want)
	}
	p := FloatOption[T](name, "").parser
	if p == nil {
		t.Errorf("%s: no float parser for %T", name, want)
	}
	if got, err := p(arg); err != nil {
		t.Errorf("%s: parse error: %v", name, err)
	} else if got != want {
		t.Errorf("%s: parse(%s): got %#v want %#v", name, arg, got, want)
	}
}
