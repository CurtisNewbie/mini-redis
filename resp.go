package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cast"
)

/*
RESP Data Types

https://redis.io/docs/reference/protocol-spec/
*/
const (
	SimpleStringsTyp = '+'
	SimpleErrorsTyp  = '-'
	IntegersTyp      = ':'
	BulkStringsTyp   = '$'
	ArraysTyp        = '*'
	NullsTyp         = '_'

	BooleansTyp        = '#'
	DoublesTyp         = ','
	BigNumbersTyp      = '('
	BulkErrorsTyp      = '('
	VerbatimStringsTyp = '='
	MapsTyp            = '%'
	SetsTyp            = '~'
	PushesTyp          = '>'
)

var (
	ErrInvalidInt             = errors.New("value is not an integer or out of range")
	ErrInvalidArgument        = errors.New("invalid arguments")
	ErrExpectArrayEle         = errors.New("invalid protocol, expecting array elements")
	ErrExpectBulkStr          = errors.New("invalid protocol, expecting bulk string")
	ErrExpectArray            = errors.New("invalid protocol, expecting Arrays type")
	ErrUnexpectedEndOfBulkStr = errors.New("invalid protocol, unexpected end of bulk string")
	ErrInvalidTypeForCommand  = errors.New("invalid data type for command")
	ErrEmptyPayload           = errors.New("invalid protocol, empty payload")

	NilVal = Value{}
)

var (
	Separator       = []byte{'\r', '\n'}
	CommandHandlers = map[string]func(args []Value) Value{
		"PING": func(args []Value) Value {
			return Value{
				typ:  SimpleStringsTyp,
				strv: "PONG",
			}
		},
		"SET": func(args []Value) Value {
			if len(args) < 2 {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}

			// TODO: Parse the args to support EX seconds | PX milliseconds
			//
			// NX | XX
			var nx bool = false
			var xx bool = false
			if len(args) > 2 {
				if args[2].typ == BulkStringsTyp {
					s := strings.ToUpper(args[2].strv)
					if s == "NX" {
						nx = true
					} else if s == "XX" {
						xx = true
					}
				}
			}
			// Debugf("SET, nx=%v, xx=%v", nx, xx)

			prev, ok := Lookup(args[0].strv)
			if (ok && nx) || (xx && !ok) { // NX and exists or XX and not exists
				return Value{typ: NullsTyp}
			}

			SetVal(args[0].strv, args[1].strv)
			if ok {
				return Value{typ: BulkStringsTyp, strv: cast.ToString(prev)}
			}
			return Value{typ: SimpleStringsTyp, strv: "OK"}
		},
		"GET": func(args []Value) Value {
			if len(args) < 1 {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}

			prev, ok := Lookup(args[0].strv)
			if !ok {
				return Value{typ: NullsTyp}
			}
			return Value{typ: BulkStringsTyp, strv: cast.ToString(prev)}
		},
		"DEL": func(args []Value) Value {
			if len(args) < 1 {
				return Value{typ: IntegersTyp, intv: 0}
			}
			cnt := int64(0)
			for _, ar := range args {
				_, ok := Lookup(ar.strv)
				if ok {
					cnt += 1
					DelKey(ar.strv)
				}
			}
			return Value{typ: IntegersTyp, intv: cnt}
		},
		"INCR": func(args []Value) Value {
			if len(args) < 1 {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}
			prev, ok := Lookup(args[0].strv)
			if !ok {
				SetVal(args[0].strv, int64(1))
				return Value{typ: IntegersTyp, intv: 1}
			}
			pv, ok := prev.(int64)
			if !ok {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidInt}
			}
			SetVal(args[0].strv, pv+1)
			return Value{typ: IntegersTyp, intv: pv + 1}
		},
		"DECR": func(args []Value) Value {
			if len(args) < 1 {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}
			prev, ok := Lookup(args[0].strv)
			if !ok {
				SetVal(args[0].strv, int64(0))
				return Value{typ: IntegersTyp, intv: 0}
			}
			pv, ok := prev.(int64)
			if !ok {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidInt}
			}
			SetVal(args[0].strv, pv-1)
			return Value{typ: IntegersTyp, intv: pv - 1}
		},
		"EXPIRE": func(args []Value) Value {
			if len(args) < 2 { // EXPIRE $KEY $SECONDS
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}

			k := args[0].strv
			_, ok := Lookup(k)
			if !ok {
				return Value{typ: IntegersTyp, intv: 0}
			}

			n := args[1].strv
			ttl := CalcExp(cast.ToInt64(n), time.Second)
			SetExpire(k, ttl)
			return Value{typ: IntegersTyp, intv: 1}
		},
		"PEXPIRE": func(args []Value) Value {
			if len(args) < 2 { // EXPIRE $KEY $MILLISECONDS
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}

			k := args[0].strv
			_, ok := Lookup(k)
			if !ok {
				return Value{typ: IntegersTyp, intv: 0}
			}

			n := args[1].strv
			ttl := CalcExp(cast.ToInt64(n), time.Millisecond)
			SetExpire(k, ttl)
			return Value{typ: IntegersTyp, intv: 1}
		},
		"TTL": func(args []Value) Value {
			if len(args) < 1 {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}

			k := args[0].strv
			_, ok := Lookup(k)
			if !ok {
				return Value{typ: IntegersTyp, intv: -2}
			}
			ttl := LoadTTL(k, false)
			return Value{typ: IntegersTyp, intv: ttl}
		},
		"PTTL": func(args []Value) Value {
			if len(args) < 1 {
				return Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}
			k := args[0].strv
			_, ok := Lookup(k)
			if !ok {
				return Value{typ: IntegersTyp, intv: -2}
			}
			ttl := LoadTTL(k, true)
			return Value{typ: IntegersTyp, intv: ttl}
		},
	}
)

var (
	TypWriter = map[byte]func(v Value, out []byte) []byte{
		BulkStringsTyp: func(v Value, out []byte) []byte {
			out = append(out, cast.ToString(len(v.strv))...)
			out = append(out, Separator...)
			out = append(out, v.strv...)
			return out
		},
		SimpleStringsTyp: func(v Value, out []byte) []byte {
			out = append(out, v.strv...)
			return out
		},
		SimpleErrorsTyp: func(v Value, out []byte) []byte {
			out = append(out, v.err.Error()...)
			return out
		},
		NullsTyp: func(v Value, out []byte) []byte {
			return out
		},
		IntegersTyp: func(v Value, out []byte) []byte {
			out = append(out, cast.ToString(v.intv)...)
			return out
		},
	}
)

type RespDataHandler func(reader *RespReader) []byte

func ParseRespData(buf []byte, handler RespDataHandler) []byte {
	return handler(&RespReader{
		Buf: buf,
		Pos: 0,
	})
}

func TokenToStr(tokens [][]byte) []string {
	st := make([]string, 0, len(tokens))
	for _, tk := range tokens {
		st = append(st, string(tk))
	}
	return st
}

func parseArray(reader *RespReader) (Value, error) {
	bufp := GetBuf()
	buf := *bufp
	defer func() {
		PutBuf(bufp)
	}()

	for {
		b, ok := reader.Peek()
		if !ok {
			break
		}
		if b >= '0' && b <= '9' {
			buf = append(buf, b)
			reader.Skip(1)
		} else {
			break
		}
	}
	if len(buf) < 1 {
		return NilVal, ErrExpectArrayEle
	}
	n := cast.ToInt(string(buf))

	elements := make([]Value, 0, n)
	for i := 0; i < n; i++ {
		ele, err := parseNext(reader)
		if err != nil {
			return NilVal, fmt.Errorf("failed to parse next array element, %w", err)
		}
		elements = append(elements, ele)
	}
	return Value{typ: ArraysTyp, arr: elements}, nil
}

func execute(v Value, err error) []byte {
	if err != nil {
		return writeErr(err)
	}

	if v.typ != ArraysTyp {
		fmt.Printf("Unable to execute, invalid data type\n")
		return writeErr(ErrInvalidTypeForCommand)
	}
	// fmt.Printf("Name Value: %#v\n", *v.arr[0])

	name := strings.ToUpper(v.arr[0].strv)
	args := v.arr[1:]
	handler := CommandHandlers[name]
	if handler == nil {
		fmt.Printf("Handler is nil for %v\n", name)
		return writeErr(fmt.Errorf("command %v is not supported", name))
	}
	if *debug {
		fmt.Printf("%v Command: %v,", NowStr(), name)
		for i, ar := range args {
			fmt.Printf("%#v,", ar)
			if i < len(args)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Print("\n")
	}
	return writeResult(handler(args))
}

func writeResult(v Value) []byte {
	out := []byte{}
	out = append(out, v.typ)
	out = TypWriter[v.typ](v, out)
	out = append(out, Separator...)
	return out
}

func writeErr(err error) []byte {
	out := []byte{}
	out = append(out, SimpleErrorsTyp)
	out = TypWriter[SimpleErrorsTyp](Value{typ: SimpleErrorsTyp, err: err}, out)
	out = append(out, Separator...)
	return out
}

func ParseRespProto(reader *RespReader) []byte {
	b, ok := reader.Peek()
	if !ok {
		return writeErr(ErrEmptyPayload)
	}
	switch b {
	case ArraysTyp:
		return execute(parseNext(reader))
	default:
		return execute(NilVal, ErrExpectArray)
	}
}

func parseNext(reader *RespReader) (Value, error) {
	reader.SkipSeparator()
	b, _ := reader.MoveOne()
	switch b {
	case BulkStringsTyp:
		return parseBulkString(reader)
	case SimpleStringsTyp:
		return parseSimpleString(reader)
	case ArraysTyp:
		return parseArray(reader)
	default:
		fmt.Printf("> parseNext: Invalid data type: '%s'\n", string(b))
		return NilVal, fmt.Errorf("invalid protocol, type %s not recognized or not supported", string(b))
	}
}

func parseSimpleString(reader *RespReader) (Value, error) {
	bufp := GetBuf()
	buf := *bufp
	defer func() {
		PutBuf(bufp)
	}()

	for {
		b, ok := reader.Peek()
		if !ok {
			break
		}
		if b == '\r' {
			b2, ok := reader.PeekNext()
			if ok && b2 == '\n' {
				reader.Skip(2)
				break
			}
		}
		buf = append(buf, b)
		reader.Skip(1)
	}
	return Value{typ: SimpleStringsTyp, strv: string(buf)}, nil
}

func parseBulkString(reader *RespReader) (Value, error) {
	bufp := GetBuf()
	buf := *bufp
	defer func() {
		PutBuf(bufp)
	}()

	for {
		b, ok := reader.Peek()
		if !ok {
			break
		}
		if b >= '0' && b <= '9' {
			buf = append(buf, b)
			reader.Skip(1)
		} else {
			break
		}
	}
	if len(buf) < 1 {
		return NilVal, ErrExpectBulkStr
	}

	n := cast.ToInt(string(buf))
	buf = buf[:0] // resuse the buffer

	reader.SkipSeparator()
	for i := 0; i < n; i++ {
		b, ok := reader.MoveOne()
		if !ok {
			return NilVal, ErrUnexpectedEndOfBulkStr
		}
		buf = append(buf, b)
	}
	return Value{typ: BulkStringsTyp, strv: string(buf)}, nil
}

type Value struct {
	typ  byte
	strv string
	intv int64
	arr  []Value
	err  error
}

type RespReader struct {
	Pos int
	Buf []byte
}

func (r *RespReader) MoveOne() (byte, bool) {
	if r.Pos >= len(r.Buf) {
		return 0, false
	}
	b := r.Buf[r.Pos]
	r.Pos += 1
	return b, true
}

func (r *RespReader) Skip(i int) {
	r.Pos += i
}

func (r *RespReader) PeekNext() (byte, bool) {
	if r.Pos+1 >= len(r.Buf) {
		return 0, false
	}
	return r.Buf[r.Pos+1], true
}

func (r *RespReader) Peek() (byte, bool) {
	return r.PeekAfter(0)
}

func (r *RespReader) PeekAfter(i int) (byte, bool) {
	if r.Pos+i >= len(r.Buf) {
		return 0, false
	}
	return r.Buf[r.Pos+i], true
}

func (r *RespReader) SkipSeparator() {
	b1, ok := r.Peek()
	if !ok {
		return
	}

	// '\r \n'
	if b1 == '\r' {
		b2, ok := r.PeekNext()
		if ok && b2 == '\n' {
			r.Skip(2)
		}
	}
}

func (r *RespReader) Rest() string {
	return string(r.Buf[r.Pos:])
}
