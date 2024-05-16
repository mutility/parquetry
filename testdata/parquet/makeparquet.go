package main

//go:generate go run makeparquet.go

import (
	"cmp"
	"os"
	"slices"
	"time"

	"github.com/parquet-go/parquet-go"
)

func main() {
	write("alphav.parquet", []struct{ A string }{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"f"}, {"g"}})
	write("alphaw.parquet", []struct{ A, B, C, D, E, F, G string }{{"a", "b", "c", "d", "e", "f", "g"}})
	type sm = map[string]string
	write("alphamapv.parquet", []struct{ M sm }{{M: sm{"A": "a"}}, {M: sm{"B": "b"}}, {M: sm{"C": "c"}}})
	write("alphamapw.parquet", []struct{ M sm }{{M: sm{"A": "a", "B": "b", "C": "c"}}})

	write("dates.parquet", []struct {
		Date int32
	}{
		{Date: 123},
		{Date: 1234},
		{Date: 12345},
	}, parquet.NewSchema("", StructOf(
		"Date", parquet.Date(),
	)))

	exSchema := parquet.NewSchema("", StructOf(
		"f", parquet.Leaf(parquet.BooleanType),
		"pf", parquet.Optional(parquet.Leaf(parquet.BooleanType)),
		"i", parquet.Int(32),
		"j", parquet.Int(32),
		"k", parquet.Int(32),
		"m", parquet.Map(parquet.String(), parquet.String()),
		"ps", parquet.Optional(parquet.String()),
		"rs", parquet.String(),
		"w", StructOf(
			"d", parquet.Date(),
			"t", parquet.Time(parquet.Millisecond),
			"s", parquet.Timestamp(parquet.Millisecond),
		),
	))
	type dt struct {
		D int32 `parquet:"d"`
		T int32 `parquet:"t"`
		S int64 `parquet:"s"`
	}
	f := false
	s := "ptr"
	write("example.parquet", []struct {
		F  bool              `parquet:"f"`
		Pf *bool             `parquet:"pf"`
		I  int               `parquet:"i"`
		J  int               `parquet:"j"`
		K  int               `parquet:"k"`
		M  map[string]string `parquet:"m"`
		Ps *string           `parquet:"ps"`
		Rs string            `parquet:"rs"`
		W  dt                `parquet:"w"`
	}{
		{F: true, Pf: &f, I: 3, J: 6, K: 9, M: map[string]string{"hello": "world"}, Rs: "aeiou", W: dt{555, 666, 777}},
		{F: false, I: 2, J: 4, K: 6, M: map[string]string{"prop": "val"}, Ps: &s, Rs: "aeiouy", W: dt{888, 999, 1000}},
	}, exSchema)

	write("times.parquet", []struct {
		Ms int64 `parquet:"ms"`
		Us int64 `parquet:"us"`
		Ns int64 `parquet:"ns"`
	}{
		{
			Ms: 12345678,
			Us: 12345678,
			Ns: 12345678,
		},
		{
			Ms: 123456789,
			Us: 123456789,
			Ns: 123456789,
		},
	}, parquet.NewSchema("", StructOf(
		"ms", parquet.Time(parquet.Millisecond),
		"us", parquet.Time(parquet.Microsecond),
		"ns", parquet.Time(parquet.Nanosecond),
	)))

	t1 := time.Date(2024, time.December, 18, 9, 23, 19, 123456789, time.UTC)
	t2 := time.Date(2012, time.July, 7, 3, 11, 45, 123456789, time.UTC)
	t3 := time.Date(2018, time.February, 22, 2, 22, 22, 123456789, time.UTC)

	write("timestamps.parquet", []struct {
		Sms int64     `parquet:",timestamp(millisecond)"`
		Sus int64     `parquet:",timestamp(microsecond)`
		Sns int64     `parquet:",timestamp(nanosecond)`
		Tms time.Time `parquet:",timestamp(millisecond)"`
		Tus time.Time `parquet:",timestamp(microsecond)`
		Tns time.Time `parquet:",timestamp(nanosecond)`
	}{
		{
			Sms: timeof[int64](t1, time.Millisecond),
			Sus: timeof[int64](t1, time.Microsecond),
			Sns: timeof[int64](t1, time.Nanosecond),
			Tms: t1,
			Tus: t1,
			Tns: t1,
		},
		{
			Sms: timeof[int64](t2, time.Millisecond),
			Sus: timeof[int64](t2, time.Microsecond),
			Sns: timeof[int64](t2, time.Nanosecond),
			Tms: t2,
			Tus: t2,
			Tns: t2,
		},
		{
			Sms: timeof[int64](t3, time.Millisecond),
			Sus: timeof[int64](t3, time.Microsecond),
			Sns: timeof[int64](t3, time.Nanosecond),
			Tms: t3,
			Tus: t3,
			Tns: t3,
		},
	}, parquet.NewSchema("", StructOf(
		"Sms", parquet.Timestamp(parquet.Millisecond),
		"Sus", parquet.Timestamp(parquet.Microsecond),
		"Sns", parquet.Timestamp(parquet.Nanosecond),
		"Tms", parquet.Timestamp(parquet.Millisecond),
		"Tus", parquet.Timestamp(parquet.Microsecond),
		"Tns", parquet.Timestamp(parquet.Nanosecond),
	)))
}

func timeof[T int32 | int64](t time.Time, dur time.Duration) T {
	return T(t.Sub(time.Unix(0, 0).UTC()) / dur)
}

func write[T any](name string, content []T, opts ...parquet.WriterOption) {
	f, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = parquet.Write(f, content, opts...)
	if err != nil {
		panic(err)
	}
	println("wrote", len(content), "records to", name)
}

type Struct struct {
	parquet.Group
	FieldOrder map[string]int
}

var _ parquet.Node = Struct{}

func (s Struct) Fields() []parquet.Field {
	fields := s.Group.Fields()
	slices.SortFunc(fields, func(a, b parquet.Field) int {
		return cmp.Compare(s.FieldOrder[a.Name()], s.FieldOrder[b.Name()])
	})
	return fields
}

func StructOf(name_fields ...any) Struct {
	kvs := name_fields
	s := Struct{make(parquet.Group, len(kvs)/2), make(map[string]int, len(kvs)/2)}
	for i := 0; i < len(kvs); i += 2 {
		s.Group[kvs[i].(string)] = kvs[i+1].(parquet.Node)
		s.FieldOrder[kvs[i].(string)] = i
	}
	return s
}
