package main

import (
	"cmp"
	"fmt"
	"reflect"
	"strings"

	"github.com/flexera/tabular/filters"
	"github.com/flexera/tabular/filters/ast"
)

func ParseReflectFilter(filter string) (ReflectFilter, error) {
	f, err := filters.Parse(filter, nil)
	return ReflectFilter{f}, err
}

type ReflectFilter struct {
	expr ast.Expr
}

func (f ReflectFilter) Eval(rec reflect.Value) (bool, error) {
	v := visitor{rec}
	x := filters.Visit(f.expr, nil, &v)
	switch x := x.(type) {
	case bool:
		return x, nil
	case error:
		return false, x
	}
	return false, fmt.Errorf("unexpected %v (%T)", x, x)
}

var _ filters.Visitor = (*visitor)(nil)

type visitor struct{ val reflect.Value }

func (v *visitor) lhs(id ast.Ident) (reflect.Value, error) {
	val := v.val
	for _, name := range strings.Split(id.Name, ".") {
		if !val.IsValid() {
			break
		}
		switch val.Kind() {
		case reflect.Struct:
			val = val.FieldByName(name)
		case reflect.Map:
			val = val.MapIndex(reflect.ValueOf(name))
		default:
			return val, fmt.Errorf("unsupported type: %v", val.Type())
		}
	}
	return val, nil
}

func (v *visitor) AcceptComparison(op ast.Operator, a ast.Ident, b ast.Lit) any {
	lhs, err := v.lhs(a)
	if err != nil {
		return err
	}

	if _, ok := b.(ast.Null); ok {
		switch op {
		case ast.EQ:
			return !lhs.IsValid() || lhs.IsNil() || lhs.Interface() == nil
		case ast.NE:
			return lhs.IsValid() && !lhs.IsNil() && lhs.Interface() != nil
		}
	}

	order := func(a, b reflect.Value, before, zero, after bool) any {
		tobool := func(cmp int) any {
			switch {
			case cmp == 0:
				return zero
			case cmp < 0:
				return before
			case cmp > 0:
				return after
			}
			return fmt.Errorf("impossible")
		}
		if v, ok := a.Interface().(interface{ Compare(any) any }); ok {
			cmp := v.Compare(b.Interface())
			switch cmp := cmp.(type) {
			case int:
				return tobool(cmp)
			case error:
				return cmp
			}
			return fmt.Errorf("unexpected comparison: %v (%T)", cmp, cmp)
		}
		switch a.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			switch b.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return tobool(cmp.Compare(a.Int(), b.Int()))
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			switch b.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				return tobool(cmp.Compare(a.Uint(), b.Uint()))
			}
		case reflect.Float32, reflect.Float64:
			switch b.Kind() {
			case reflect.Float32, reflect.Float64:
				return tobool(cmp.Compare(a.Float(), b.Float()))
			}
		case reflect.String:
			switch b.Kind() {
			case reflect.String:
				return tobool(cmp.Compare(a.String(), b.String()))
			}
		}
		return fmt.Errorf("incompatible types: %s and %s", a.Type(), b.Type())
	}

	memberOf := func(a, b reflect.Value, in bool) any {
		switch a.Kind() {
		case reflect.Slice, reflect.Array:
			for i, I := 0, a.Len(); i < I; i++ {
				if a.Index(i).Equal(b) {
					return in
				}
			}
			return !in
		case reflect.Map:
			m := a.MapRange()
			for m.Next() {
				if m.Key().Equal(b) {
					return in
				}
			}
			return !in
		}
		return fmt.Errorf("incompatible types: %s and %s", a.Type(), b.Type())
	}

	rhs := reflect.ValueOf(b.Value())

	switch op {
	case ast.EQ:
		return lhs.Equal(rhs)
	case ast.NE:
		return !lhs.Equal(rhs)
	case ast.LT:
		return order(lhs, rhs, true, false, false)
	case ast.LE:
		return order(lhs, rhs, true, true, false)
	case ast.GT:
		return order(lhs, rhs, false, false, true)
	case ast.GE:
		return order(lhs, rhs, false, true, true)
	case ast.CO:
		return strings.Contains(lhs.String(), rhs.String())
	case ast.NC:
		return !strings.Contains(lhs.String(), rhs.String())
	case ast.SW:
		return strings.HasPrefix(lhs.String(), rhs.String())
	case ast.EW:
		return strings.HasSuffix(lhs.String(), rhs.String())
	case ast.IN:
		return memberOf(rhs, lhs, true)
	case ast.NI:
		return memberOf(rhs, lhs, false)
	}
	return fmt.Errorf("unsupported comparison: %v", op)
}

func (v *visitor) AcceptMembership(op ast.Operator, a ast.Ident, b []ast.Lit) any {
	lhs, err := v.lhs(a)
	if err != nil {
		return err
	}
	if op != ast.IN && op != ast.NI {
		return fmt.Errorf("unsupported comparison: %v", op)
	}

	for _, rhs := range b {
		if reflect.ValueOf(rhs.Value()).Equal(lhs) {
			return op == ast.IN
		}
	}
	return op != ast.IN
}

func (v *visitor) AcceptGroup(op ast.Grouping, a any, b any) any {
	switch a := a.(type) {
	case error:
		return a
	case bool:
		if op == ast.NOT {
			return !a
		}
		switch b := b.(type) {
		case error:
			return b
		case bool:
			switch op {
			case ast.AND:
				return a && b
			case ast.OR:
				return a || b
			}
			return fmt.Errorf("unsupported grouping: %v", op)
		}
		return fmt.Errorf("unexpected %v", b)
	}
	return fmt.Errorf("unexpected %v", a)
}
