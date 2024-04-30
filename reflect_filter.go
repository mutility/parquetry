package main

import (
	"cmp"
	"fmt"
	"reflect"
	"strings"

	"github.com/flexera/tabular/filters"
	"github.com/flexera/tabular/filters/ast"
)

type ReflectFilter func(reflect.Value) bool

func ParseReflectFilter(filter string) (ReflectFilter, error) {
	f, err := filters.Parse(filter, nil)
	if err != nil {
		return nil, err
	}

	v := visitor{}
	x := filters.Visit(f, nil, &v)
	if err, ok := x.(error); ok && err != nil {
		return nil, err
	}

	return x.(ReflectFilter), nil
}

var _ filters.Visitor = (*visitor)(nil)

type visitor struct{}

func (v *visitor) AcceptComparison(op ast.Operator, a ast.Ident, b ast.Lit) any {
	lhs := lookupID(a)

	if _, ok := b.(ast.Null); ok {
		switch op {
		case ast.EQ:
			return ReflectFilter(func(v reflect.Value) bool { return lhs(v).Interface() == nil })
		case ast.NE:
			return ReflectFilter(func(v reflect.Value) bool { return lhs(v).Interface() != nil })
		}
	}

	order := func(a, b reflect.Value) int {
		if v, ok := a.Interface().(interface{ Compare(any) int }); ok {
			return v.Compare(b.Interface())
		}
		switch a.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return cmp.Compare(a.Int(), b.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return cmp.Compare(a.Uint(), b.Uint())
		case reflect.Float32, reflect.Float64:
			return cmp.Compare(a.Float(), b.Float())
		case reflect.String:
			return cmp.Compare(a.String(), b.String())
		}
		panic(fmt.Sprint("unsupported type:", a.Type()))
	}

	memberOf := func(a, b reflect.Value) bool {
		switch a.Kind() {
		case reflect.Slice, reflect.Array:
			for i, I := 0, a.Len(); i < I; i++ {
				if a.Index(i).Equal(b) {
					return true
				}
			}
			return false
		case reflect.Map:
			m := a.MapRange()
			for m.Next() {
				if m.Key().Equal(b) {
					return true
				}
			}
			return false
		}
		panic(fmt.Sprint("unsupported type:", a.Type()))
	}

	rhs := reflect.ValueOf(b.Value())

	switch op {
	case ast.EQ:
		return ReflectFilter(func(v reflect.Value) bool { return lhs(v).Equal(rhs) })
	case ast.NE:
		return ReflectFilter(func(v reflect.Value) bool { return !lhs(v).Equal(rhs) })
	case ast.LT:
		return ReflectFilter(func(v reflect.Value) bool { return order(lhs(v), rhs) < 0 })
	case ast.LE:
		return ReflectFilter(func(v reflect.Value) bool { return order(lhs(v), rhs) <= 0 })
	case ast.GT:
		return ReflectFilter(func(v reflect.Value) bool { return order(lhs(v), rhs) > 0 })
	case ast.GE:
		return ReflectFilter(func(v reflect.Value) bool { return order(lhs(v), rhs) >= 0 })
	case ast.CO:
		return ReflectFilter(func(v reflect.Value) bool { return strings.Contains(lhs(v).String(), rhs.String()) })
	case ast.NC:
		return ReflectFilter(func(v reflect.Value) bool { return !strings.Contains(lhs(v).String(), rhs.String()) })
	case ast.SW:
		return ReflectFilter(func(v reflect.Value) bool { return strings.HasPrefix(lhs(v).String(), rhs.String()) })
	case ast.EW:
		return ReflectFilter(func(v reflect.Value) bool { return strings.HasSuffix(lhs(v).String(), rhs.String()) })
	case ast.IN:
		return ReflectFilter(func(v reflect.Value) bool { return memberOf(rhs, lhs(v)) })
	case ast.NI:
		return ReflectFilter(func(v reflect.Value) bool { return !memberOf(rhs, lhs(v)) })
	}
	return fmt.Errorf("unsupported comparison: %v", op)
}

func lookupID(id ast.Ident) func(reflect.Value) reflect.Value {
	return func(v reflect.Value) reflect.Value {
		for _, name := range strings.Split(id.Name, ".") {
			if !v.IsValid() {
				break
			}
			switch v.Kind() {
			case reflect.Struct:
				v = v.FieldByName(name)
			case reflect.Map:
				v = v.MapIndex(reflect.ValueOf(name))
			}
		}
		return v
	}
}

func (v *visitor) AcceptMembership(op ast.Operator, a ast.Ident, b []ast.Lit) any {
	lhs := lookupID(a)

	memberOf := func(v reflect.Value) bool {
		for _, r := range b {
			if reflect.ValueOf(r.Value()).Equal(v) {
				return true
			}
		}
		return false
	}

	switch op {
	case ast.IN:
		return ReflectFilter(func(v reflect.Value) bool { return memberOf(lhs(v)) })
	case ast.NI:
		return ReflectFilter(func(v reflect.Value) bool { return !memberOf(lhs(v)) })
	}

	return fmt.Errorf("unsupported comparison: %v", op)
}

func (v *visitor) AcceptGroup(op ast.Grouping, a any, b any) any {
	fa := a.(ReflectFilter)
	if op == ast.NOT {
		return ReflectFilter(func(v reflect.Value) bool { return !fa(v) })
	}
	fb := b.(ReflectFilter)
	switch op {
	case ast.AND:
		return ReflectFilter(func(v reflect.Value) bool { return fa(v) && fb(v) })
	case ast.OR:
		return ReflectFilter(func(v reflect.Value) bool { return fa(v) || fb(v) })
	}
	return fmt.Errorf("unsupported grouping: %v", op)
}
