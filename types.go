package main

import "time"

type (
	LocDate       int32
	UTCDate       int32
	TimeMilliLoc  int64
	TimeMilliUTC  int64
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
	rfc3339Nano  = "2006-01-02T15:04:05.999999999Z07:00"
	rfc3339Micro = "2006-01-02T15:04:05.999999Z07:00"
	rfc3339Milli = "2006-01-02T15:04:05.999Z07:00"
)

func epochTime(offset time.Duration) time.Time {
	return time.Unix(0, 0).Add(offset)
}

func epochString[T interface {
	offset() time.Duration
	loc() *time.Location
	layout() string
}](t T) string {
	return epochTime(t.offset()).In(t.loc()).Format(t.layout())
}

func marshalEpoch[T interface {
	offset() time.Duration
	loc() *time.Location
	layout() string
}](t T) ([]byte, error) {
	return epochTime(t.offset()).In(t.loc()).AppendFormat(nil, t.layout()), nil
}

func (t LocDate) String() string       { return epochString(t) }
func (t UTCDate) String() string       { return epochString(t) }
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

func (t LocDate) MarshalText() ([]byte, error)       { return marshalEpoch(t) }
func (t UTCDate) MarshalText() ([]byte, error)       { return marshalEpoch(t) }
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

func (t LocDate) offset() time.Duration       { return time.Duration(t) * t.unit() }
func (t UTCDate) offset() time.Duration       { return time.Duration(t) * t.unit() }
func (t StampMilliLoc) offset() time.Duration { return time.Duration(t) * t.unit() }
func (t StampMilliUTC) offset() time.Duration { return time.Duration(t) * t.unit() }
func (t StampMicroLoc) offset() time.Duration { return time.Duration(t) * t.unit() }
func (t StampMicroUTC) offset() time.Duration { return time.Duration(t) * t.unit() }
func (t StampNanoLoc) offset() time.Duration  { return time.Duration(t) * t.unit() }
func (t StampNanoUTC) offset() time.Duration  { return time.Duration(t) * t.unit() }
func (t TimeMilliLoc) offset() time.Duration  { return time.Duration(t) * t.unit() }
func (t TimeMilliUTC) offset() time.Duration  { return time.Duration(t) * t.unit() }
func (t TimeMicroLoc) offset() time.Duration  { return time.Duration(t) * t.unit() }
func (t TimeMicroUTC) offset() time.Duration  { return time.Duration(t) * t.unit() }
func (t TimeNanoLoc) offset() time.Duration   { return time.Duration(t) * t.unit() }
func (t TimeNanoUTC) offset() time.Duration   { return time.Duration(t) * t.unit() }

func (LocDate) unit() time.Duration       { return 24 * time.Hour }
func (UTCDate) unit() time.Duration       { return 24 * time.Hour }
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

func (LocDate) loc() *time.Location       { return time.Local }
func (UTCDate) loc() *time.Location       { return time.UTC }
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

func (LocDate) layout() string       { return time.DateOnly }
func (UTCDate) layout() string       { return time.DateOnly }
func (StampMilliLoc) layout() string { return rfc3339Milli }
func (StampMilliUTC) layout() string { return rfc3339Milli }
func (StampMicroLoc) layout() string { return rfc3339Micro }
func (StampMicroUTC) layout() string { return rfc3339Micro }
func (StampNanoLoc) layout() string  { return rfc3339Nano }
func (StampNanoUTC) layout() string  { return rfc3339Nano }
func (TimeMilliLoc) layout() string  { return rfc3339Milli }
func (TimeMilliUTC) layout() string  { return rfc3339Milli }
func (TimeMicroLoc) layout() string  { return rfc3339Micro }
func (TimeMicroUTC) layout() string  { return rfc3339Micro }
func (TimeNanoLoc) layout() string   { return rfc3339Nano }
func (TimeNanoUTC) layout() string   { return rfc3339Nano }
