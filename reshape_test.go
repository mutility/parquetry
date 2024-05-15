package main

import (
	"reflect"
	"testing"
)

var reshapeTests = []struct {
	Shape string
	Value any
	Want  []reValue
}{
	{"A", struct{ A string }{"a"}, []reValue{reField{"A", ""}}},
	{"A AS A", struct{ A string }{"a"}, []reValue{reField{"A", "A"}}},
	{"A AS B", struct{ B string }{"a"}, []reValue{reField{"A", "B"}}},
	{"B", struct{ B int32 }{'b'}, []reValue{reField{"B", ""}}},
	{"B AS A", struct{ A int32 }{'b'}, []reValue{reField{"B", "A"}}},
	{"B AS B", struct{ B int32 }{'b'}, []reValue{reField{"B", "B"}}},
	{"A, B", ab, []reValue{reField{"A", ""}, reField{"B", ""}}},
	{"B, A", ba, []reValue{reField{"B", ""}, reField{"A", ""}}},
	{"D", def, []reValue{reField{"D", ""}}},
	{"D.E, D.F", dedf, []reValue{reField{"D.E", ""}, reField{"D.F", ""}}},
	{"B, (A, C) AS G", acg, []reValue{reField{"B", ""}, reStruct{[]reValue{reField{"A", ""}, reField{"C", ""}}, "G"}}},
}

var (
	ab = struct {
		A string
		B int32
	}{value.A, value.B}
	ba = struct {
		B int32
		A string
	}{value.B, value.A}
	def = struct {
		D schemaD
	}{D: value.D}
	dedf = struct {
		E int32
		F int64
	}{value.D.E, value.D.F}
	acg = struct {
		B int32
		G struct {
			A string
			C map[string]int64
		}
	}{
		B: value.B,
		G: struct {
			A string
			C map[string]int64
		}{
			A: value.A,
			C: value.C,
		},
	}
)

type schemaType = struct {
	A string
	B int32
	C map[string]int64
	D schemaD
}
type schemaD = struct {
	E int32
	F int64
}

var value = schemaType{
	"a", 'b', map[string]int64{"c": 'c'}, struct {
		E int32
		F int64
	}{'e', 'f'},
}

var schema = reflect.TypeOf(value)

func TestParse(t *testing.T) {
	for _, tt := range reshapeTests {
		t.Run(tt.Shape, func(t *testing.T) {
			reshape, err := ParseShape(Shape(tt.Shape), schema)
			if err != nil {
				t.Fatal(err)
			}
			if gt, wt := reshape.Type().String(), reflect.TypeOf(tt.Value).String(); gt != wt {
				t.Errorf("type: got %v want %v", gt, wt)
			}
			if !reflect.DeepEqual(reshape.fields, tt.Want) {
				t.Errorf("shape: got %+v want %+v", reshape.fields, tt.Want)
			}
			if v, err := reshape.Eval(reflect.ValueOf(value)); err != nil {
				t.Errorf("value: %v", err)
			} else if !reflect.DeepEqual(v.Interface(), tt.Value) {
				t.Errorf("value: got %+v want %+v", v, tt.Value)
			}
		})
	}
}
