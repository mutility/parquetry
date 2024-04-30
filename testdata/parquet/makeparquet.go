package main

//go:generate go run makeparquet.go

import (
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
)

func main() {
	write("alphav.parquet", []struct{ A string }{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"f"}, {"g"}})
	write("alphaw.parquet", []struct{ A, B, C, D, E, F, G string }{{"a", "b", "c", "d", "e", "f", "g"}})
	type sm = map[string]string
	write("alphamapv.parquet", []struct{ M sm }{{M: sm{"A": "a"}}, {M: sm{"B": "b"}}, {M: sm{"C": "c"}}})
	write("alphamapw.parquet", []struct{ M sm }{{M: sm{"A": "a", "B": "b", "C": "c"}}})

	t := time.Date(2024, time.December, 18, 9, 23, 19, 123456789, time.UTC)
	write("times.parquet", []struct {
		Date  int32     `parquet:",date"`
		Stamp int64     `parquet:",timestamp"`
		Sms   int64     `parquet:",timestamp(millisecond)"`
		Sus   int64     `parquet:",timestamp(microsecond)`
		Sns   int64     `parquet:",timestamp(nanosecond)`
		Time  time.Time `parquet:",timestamp"`
		Tms   time.Time `parquet:",timestamp(millisecond)"`
		Tus   time.Time `parquet:",timestamp(microsecond)`
		Tns   time.Time `parquet:",timestamp(nanosecond)`
	}{
		{
			Date:  timeof[int32](t, 24*time.Hour),
			Stamp: timeof[int64](t, time.Millisecond),
			Sms:   timeof[int64](t, time.Millisecond),
			Sus:   timeof[int64](t, time.Microsecond),
			Sns:   timeof[int64](t, time.Nanosecond),
			Time:  t,
			Tms:   t,
			Tus:   t,
			Tns:   t,
		},
	})
}

func timeof[T int32 | int64](t time.Time, dur time.Duration) T {
	return T(t.Sub(time.Unix(0, 0).UTC()) / dur)
}

func write[T any](name string, content []T) {
	f, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = parquet.Write(f, content)
	if err != nil {
		panic(err)
	}
}
