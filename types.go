package main

import (
	"cmp"
	"fmt"
	"time"
)

type (
	Date          int32
	TimeMilliLoc  int32
	TimeMilliUTC  int32
	TimeMicroLoc  int64
	TimeMicroUTC  int64
	TimeNanoLoc   int64
	TimeNanoUTC   int64
	StampMilliLoc int64
	StampMilliUTC int64
	StampMicroLoc int64
	StampMicroUTC int64
	StampNanoLoc  int64
	StampNanoUTC  int64
)

const (
	fullRFC3339Nano      = "2006-01-02T15:04:05.999999999Z07:00"
	fullRFC3339Micro     = "2006-01-02T15:04:05.999999Z07:00"
	fullRFC3339Milli     = "2006-01-02T15:04:05.999Z07:00"
	timeOnlyRFC3339Nano  = "15:04:05.999999999Z07:00"
	timeOnlyRFC3339Micro = "15:04:05.999999Z07:00"
	timeOnlyRFC3339Milli = "15:04:05.999Z07:00"
)

func epochTime(offset time.Duration) time.Time {
	return time.Unix(0, 0).Add(offset)
}

type inttime interface {
	~int32 | ~int64
	unit() time.Duration
	loc() *time.Location
	layout() string
}

func epochString[T inttime](t T) string {
	return epochTime(time.Duration(t) * t.unit()).In(t.loc()).Format(t.layout())
}

func epochCompare[T inttime](a T, b any) (int, error) {
	switch b := b.(type) {
	case time.Time:
		return cmp.Compare(time.Duration(a), b.Sub(time.Unix(0, 0))/a.unit()), nil
	case string:
		t, err := time.ParseInLocation(a.layout(), b, a.loc())
		if err != nil {
			return 0, err
		}
		return cmp.Compare(time.Duration(a), t.Sub(time.Unix(0, 0))/a.unit()), nil
	case int:
		return cmp.Compare(int64(a), int64(b)), nil
	case T:
		return cmp.Compare(int64(a), int64(b)), nil
	}
	return 0, fmt.Errorf("unsupported comparison type for %T: %T", a, b)
}

func timeCompare[T inttime](a T, b any) (int, error) {
	switch b := b.(type) {
	case time.Duration:
		return cmp.Compare(time.Duration(a), b/a.unit()), nil
	case string:
		d, err := time.ParseDuration(b)
		if err != nil {
			return 0, err
		}
		return cmp.Compare(time.Duration(a), d/a.unit()), nil
	case int:
		return cmp.Compare(int64(a), int64(b)), nil
	case T:
		return cmp.Compare(int64(a), int64(b)), nil
	}
	return 0, fmt.Errorf("unsupported comparison type for %T: %T", a, b)
}

func marshalEpoch[T inttime](t T) ([]byte, error) {
	return epochTime(time.Duration(t)*t.unit()).In(t.loc()).AppendFormat(nil, t.layout()), nil
}

func (t Date) String() string          { return epochString(t) }
func (t StampMilliLoc) String() string { return epochString(t) }
func (t StampMilliUTC) String() string { return epochString(t) }
func (t StampMicroLoc) String() string { return epochString(t) }
func (t StampMicroUTC) String() string { return epochString(t) }
func (t StampNanoLoc) String() string  { return epochString(t) }
func (t StampNanoUTC) String() string  { return epochString(t) }
func (t TimeMilliLoc) String() string  { return epochString(t) }
func (t TimeMilliUTC) String() string  { return epochString(t) }
func (t TimeMicroLoc) String() string  { return epochString(t) }
func (t TimeMicroUTC) String() string  { return epochString(t) }
func (t TimeNanoLoc) String() string   { return epochString(t) }
func (t TimeNanoUTC) String() string   { return epochString(t) }

func (t Date) MarshalText() ([]byte, error)          { return marshalEpoch(t) }
func (t StampMilliLoc) MarshalText() ([]byte, error) { return marshalEpoch(t) }
func (t StampMilliUTC) MarshalText() ([]byte, error) { return marshalEpoch(t) }
func (t StampMicroLoc) MarshalText() ([]byte, error) { return marshalEpoch(t) }
func (t StampMicroUTC) MarshalText() ([]byte, error) { return marshalEpoch(t) }
func (t StampNanoLoc) MarshalText() ([]byte, error)  { return marshalEpoch(t) }
func (t StampNanoUTC) MarshalText() ([]byte, error)  { return marshalEpoch(t) }
func (t TimeMilliLoc) MarshalText() ([]byte, error)  { return marshalEpoch(t) }
func (t TimeMilliUTC) MarshalText() ([]byte, error)  { return marshalEpoch(t) }
func (t TimeMicroLoc) MarshalText() ([]byte, error)  { return marshalEpoch(t) }
func (t TimeMicroUTC) MarshalText() ([]byte, error)  { return marshalEpoch(t) }
func (t TimeNanoLoc) MarshalText() ([]byte, error)   { return marshalEpoch(t) }
func (t TimeNanoUTC) MarshalText() ([]byte, error)   { return marshalEpoch(t) }

func (Date) unit() time.Duration          { return 24 * time.Hour }
func (StampMilliLoc) unit() time.Duration { return time.Millisecond }
func (StampMilliUTC) unit() time.Duration { return time.Millisecond }
func (StampMicroLoc) unit() time.Duration { return time.Microsecond }
func (StampMicroUTC) unit() time.Duration { return time.Microsecond }
func (StampNanoLoc) unit() time.Duration  { return time.Nanosecond }
func (StampNanoUTC) unit() time.Duration  { return time.Nanosecond }
func (TimeMilliLoc) unit() time.Duration  { return time.Millisecond }
func (TimeMilliUTC) unit() time.Duration  { return time.Millisecond }
func (TimeMicroLoc) unit() time.Duration  { return time.Microsecond }
func (TimeMicroUTC) unit() time.Duration  { return time.Microsecond }
func (TimeNanoLoc) unit() time.Duration   { return time.Nanosecond }
func (TimeNanoUTC) unit() time.Duration   { return time.Nanosecond }

func (Date) loc() *time.Location          { return time.UTC }
func (StampMilliLoc) loc() *time.Location { return time.Local }
func (StampMilliUTC) loc() *time.Location { return time.UTC }
func (StampMicroLoc) loc() *time.Location { return time.Local }
func (StampMicroUTC) loc() *time.Location { return time.UTC }
func (StampNanoLoc) loc() *time.Location  { return time.Local }
func (StampNanoUTC) loc() *time.Location  { return time.UTC }
func (TimeMilliLoc) loc() *time.Location  { return time.Local }
func (TimeMilliUTC) loc() *time.Location  { return time.UTC }
func (TimeMicroLoc) loc() *time.Location  { return time.Local }
func (TimeMicroUTC) loc() *time.Location  { return time.UTC }
func (TimeNanoLoc) loc() *time.Location   { return time.Local }
func (TimeNanoUTC) loc() *time.Location   { return time.UTC }

func (Date) layout() string          { return time.DateOnly }
func (StampMilliLoc) layout() string { return fullRFC3339Milli }
func (StampMilliUTC) layout() string { return fullRFC3339Milli }
func (StampMicroLoc) layout() string { return fullRFC3339Micro }
func (StampMicroUTC) layout() string { return fullRFC3339Micro }
func (StampNanoLoc) layout() string  { return fullRFC3339Nano }
func (StampNanoUTC) layout() string  { return fullRFC3339Nano }
func (TimeMilliLoc) layout() string  { return timeOnlyRFC3339Milli }
func (TimeMilliUTC) layout() string  { return timeOnlyRFC3339Milli }
func (TimeMicroLoc) layout() string  { return timeOnlyRFC3339Micro }
func (TimeMicroUTC) layout() string  { return timeOnlyRFC3339Micro }
func (TimeNanoLoc) layout() string   { return timeOnlyRFC3339Nano }
func (TimeNanoUTC) layout() string   { return timeOnlyRFC3339Nano }
