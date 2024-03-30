package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"

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
	ErrExpectArrayEle         = errors.New("invalid protocol, expected array elements")
	ErrExpectBulkStr          = errors.New("invalid protocol, expected bulk string")
	ErrExpectArray            = errors.New("invalid protocol, expecting Arrays type")
	ErrUnexpectedEndOfBulkStr = errors.New("invalid protocol, unexpected end of bulk string")
	ErrInvalidTypeForCommand  = errors.New("invalid data type for command")
)

var (
	mem   = map[string]any{}
	memRw = sync.RWMutex{}
)

var (
	Separator       = []byte{'\r', '\n'}
	CommandHandlers = map[string]func(args []*Value) *Value{
		"PING": func(args []*Value) *Value {
			return &Value{
				typ:  SimpleStringsTyp,
				strv: "PONG",
			}
		},
		"SET": func(args []*Value) *Value {
			if len(args) < 2 {
				return &Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}

			memRw.Lock()
			defer memRw.Unlock()
			prev, ok := mem[args[0].strv]
			mem[args[0].strv] = args[1].strv
			if ok {
				return &Value{typ: BulkStringsTyp, strv: cast.ToString(prev)}
			}
			return &Value{typ: SimpleStringsTyp, strv: "OK"}
		},
		"GET": func(args []*Value) *Value {
			if len(args) < 1 {
				return &Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}

			memRw.RLock()
			defer memRw.RUnlock()
			prev, ok := mem[args[0].strv]
			if !ok {
				return &Value{typ: NullsTyp}
			}
			return &Value{typ: BulkStringsTyp, strv: cast.ToString(prev)}
		},
		"DEL": func(args []*Value) *Value {
			if len(args) < 1 {
				return &Value{typ: IntegersTyp, intv: 0}
			}

			memRw.Lock()
			defer memRw.Unlock()
			cnt := int64(0)
			for _, ar := range args {
				_, ok := mem[ar.strv]
				if ok {
					cnt += 1
					delete(mem, ar.strv)
				}
			}
			return &Value{typ: IntegersTyp, intv: cnt}
		},
		"INCR": func(args []*Value) *Value {
			if len(args) < 1 {
				return &Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}
			memRw.Lock()
			defer memRw.Unlock()
			prev, ok := mem[args[0].strv]
			if !ok {
				mem[args[0].strv] = int64(1)
				return &Value{typ: IntegersTyp, intv: 1}
			}
			pv, ok := prev.(int64)
			if !ok {
				return &Value{typ: SimpleErrorsTyp, err: ErrInvalidInt}
			}
			mem[args[0].strv] = pv + 1
			return &Value{typ: IntegersTyp, intv: pv + 1}
		},
		"DECR": func(args []*Value) *Value {
			if len(args) < 1 {
				return &Value{typ: SimpleErrorsTyp, err: ErrInvalidArgument}
			}
			memRw.Lock()
			defer memRw.Unlock()
			prev, ok := mem[args[0].strv]
			if !ok {
				mem[args[0].strv] = int64(0)
				return &Value{typ: IntegersTyp, intv: 0}
			}
			pv, ok := prev.(int64)
			if !ok {
				return &Value{typ: SimpleErrorsTyp, err: ErrInvalidInt}
			}
			mem[args[0].strv] = pv - 1
			return &Value{typ: IntegersTyp, intv: pv - 1}
		},
	}
)

var (
	TypWriter = map[byte]func(v *Value, out []byte) []byte{
		BulkStringsTyp: func(v *Value, out []byte) []byte {
			out = append(out, cast.ToString(len(v.strv))...)
			out = append(out, Separator...)
			out = append(out, v.strv...)
			return out
		},
		SimpleStringsTyp: func(v *Value, out []byte) []byte {
			out = append(out, v.strv...)
			return out
		},
		SimpleErrorsTyp: func(v *Value, out []byte) []byte {
			out = append(out, v.err.Error()...)
			return out
		},
		NullsTyp: func(v *Value, out []byte) []byte {
			return out
		},
		IntegersTyp: func(v *Value, out []byte) []byte {
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

func parseArray(reader *RespReader) (*Value, error) {
	b, ok := reader.ReadByte()
	if !ok {
		return nil, ErrExpectArrayEle
	}
	n := cast.ToInt(string(b))

	elements := make([]*Value, 0, n)
	for i := 0; i < n; i++ {
		ele, err := parseNext(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to parse next array element, %w", err)
		}
		elements = append(elements, ele)
	}
	return &Value{typ: ArraysTyp, arr: elements}, nil
}

func execute(v *Value, err error) []byte {
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
	return writeResult(handler(args))
}

func writeResult(v *Value) []byte {
	out := []byte{}
	out = append(out, v.typ)
	out = TypWriter[v.typ](v, out)
	out = append(out, Separator...)
	return out
}

func writeErr(err error) []byte {
	out := []byte{}
	out = append(out, SimpleErrorsTyp)
	out = TypWriter[SimpleErrorsTyp](&Value{typ: SimpleErrorsTyp, err: err}, out)
	out = append(out, Separator...)
	return out
}

func ParseRespProto(reader *RespReader) []byte {
	b, _ := reader.Peek()
	switch b {
	case ArraysTyp:
		return execute(parseNext(reader))
	default:
		return execute(nil, ErrExpectArray)
	}
}

func parseNext(reader *RespReader) (*Value, error) {
	reader.SkipSeparator()
	b, _ := reader.ReadByte()
	switch b {
	case BulkStringsTyp:
		return parseBulkString(reader)
	case SimpleStringsTyp:
		return parseSimpleString(reader)
	case ArraysTyp:
		return parseArray(reader)
	default:
		fmt.Printf("> parseNext: Invalid data type: '%s'\n", string(b))
		return nil, fmt.Errorf("invalid protocol, type %s not recognized or not supported", string(b))
	}
}

func parseSimpleString(reader *RespReader) (*Value, error) {
	buf := []byte{}

	for {
		b, ok := reader.Peek()
		if !ok {
			break
		}
		if b == '\r' {
			b2, ok := reader.PeekNext()
			if ok && b2 == '\n' {
				reader.Move(2)
				break
			}
		}
		buf = append(buf, b)
		reader.Move(1)
	}
	return &Value{strv: string(buf)}, nil
}

func parseBulkString(reader *RespReader) (*Value, error) {
	b, ok := reader.ReadByte()
	if !ok {
		return nil, ErrExpectBulkStr
	}
	buf := []byte{}
	n := cast.ToInt(string(b))
	reader.SkipSeparator()
	for i := 0; i < n; i++ {
		b, ok = reader.ReadByte()
		if !ok {
			return nil, ErrUnexpectedEndOfBulkStr
		}
		buf = append(buf, b)
	}
	return &Value{strv: string(buf)}, nil
}

type Value struct {
	typ  byte
	strv string
	intv int64
	arr  []*Value
	err  error
}

type RespReader struct {
	Pos int
	Buf []byte
}

func (r *RespReader) ReadByte() (byte, bool) {
	if r.Pos >= len(r.Buf) {
		return 0, false
	}
	b := r.Buf[r.Pos]
	r.Pos += 1
	return b, true
}

func (r *RespReader) Move(i int) {
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
			r.Move(2)
		}
	}
}

func (r *RespReader) Rest() string {
	return string(r.Buf[r.Pos:])
}
