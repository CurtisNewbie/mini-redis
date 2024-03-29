package resp

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
)

/*
RESP Data Types

https://redis.io/docs/reference/protocol-spec/
*/
const (
	SimpleStringsTyp   = '+'
	SimpleErrorsTyp    = '-'
	IntegersTyp        = ':'
	BulkStringsTyp     = '$'
	ArraysTyp          = '*'
	NullsTyp           = '_'
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
	Separator       = []byte{'\r', '\n'}
	CommandHandlers = map[string]func(args []*Value) *Value{
		"PING": func(args []*Value) *Value {
			return &Value{
				typ:     BulkStringsTyp,
				bulkstr: "PONG",
			}
		},
	}
)

type RespDataHandler func(reader *RespReader) []byte

func ParseRespData(buf []byte, handler RespDataHandler) []byte {
	// fmt.Printf("> typ:%s, tokens:\n%s\n", string(buf[0]), buf)
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

func parseArray(reader *RespReader) *Value {
	b, _ := reader.ReadByte()
	n := cast.ToInt(string(b))

	reader.SkipSeparator()

	// fmt.Printf("Rest, count: %v, >>%v<<\n", n, reader.Rest())

	elements := make([]*Value, 0, n)
	for i := 0; i < n; i++ {
		elements = append(elements, parseNext(reader))
	}
	// fmt.Printf("Parsed array: %#v\n", elements)
	return &Value{
		typ: ArraysTyp,
		arr: elements,
	}
}

func execute(v *Value) []byte {
	if v.typ != ArraysTyp {
		fmt.Printf("Unable to execute, invalid data type\n")
		return nil
	}
	// fmt.Printf("Name Value: %#v\n", *v.arr[0])

	name := strings.ToUpper(v.arr[0].bulkstr)
	args := v.arr[1:]
	handler := CommandHandlers[name]
	if handler == nil {
		fmt.Printf("Handler is nil for %v\n", name)
		return nil
	}
	return writeResult(handler(args))
}

func writeResult(v *Value) []byte {
	out := []byte{}
	switch v.typ {
	case BulkStringsTyp:
		out = append(out, BulkStringsTyp)
		out = append(out, cast.ToString(len(v.bulkstr))...)
		out = append(out, Separator...)
		out = append(out, v.bulkstr...)
		return out
	}

	return nil
}

func ParseRespProto(reader *RespReader) []byte {
	b, _ := reader.Peek()
	// fmt.Printf("Parsing proto, peek: %v\n", b)
	switch b {
	case ArraysTyp:
		fmt.Println("Parsing proto ArraysTyp")
		return execute(parseNext(reader))
	default:
		fmt.Printf("> ParseRespProto: Invalid data type: '%s'\n", string(b))
		return nil
	}
}

func parseNext(reader *RespReader) *Value {
	reader.SkipSeparator()
	// fmt.Printf("parseNext, Rest: >>'%v'<<\n", reader.Rest())

	b, _ := reader.ReadByte()
	switch b {
	case BulkStringsTyp:
		// fmt.Println("BulkStrings")
		return parseBulkString(reader)
	case ArraysTyp:
		// fmt.Println("Arrays")
		return parseArray(reader)
	default:
		fmt.Printf("> parseNext: Invalid data type: '%s'\n", string(b))
		return nil
	}
}

func parseBulkString(reader *RespReader) *Value {
	b, _ := reader.ReadByte()
	buf := []byte{}
	n := cast.ToInt(string(b))
	reader.SkipSeparator()
	for i := 0; i < n; i++ {
		b, _ = reader.ReadByte()
		buf = append(buf, b)
	}
	// fmt.Println("Parsed BulkString")
	return &Value{
		bulkstr: string(buf),
	}
}

type Value struct {
	typ     byte
	strv    string
	bulkstr string
	intv    int
	arr     []*Value
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
