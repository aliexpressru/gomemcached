// nolint
package memcached

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandCodeString(t *testing.T) {
	if GET.String() != "GET" {
		t.Fatalf("Expected \"GET\" for GET, got \"%v\"", GET.String())
	}

	cc := OpCode(0x80)
	if cc.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", cc.String())
	}
}

func TestStatusNameString(t *testing.T) {
	if SUCCESS.String() != "SUCCESS" {
		t.Fatalf("Expected \"SUCCESS\" for SUCCESS, got \"%v\"",
			SUCCESS.String())
	}

	s := Status(0x80)
	if s.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", s.String())
	}
}

func TestIsQuiet(t *testing.T) {
	for v, k := range CommandNames {
		isq := strings.HasSuffix(k, "Q")
		if v.IsQuiet() != isq {
			t.Errorf("Expected quiet=%v for %v, got %v",
				isq, v, v.IsQuiet())
		}
	}
}

func Test_prepareAuthData(t *testing.T) {
	type args struct {
		user string
		pass string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "1", args: args{
				user: "testuser",
				pass: "testpass",
			},
			want: []byte("\x00testuser\x00testpass"),
		},
		{
			name: "2", args: args{
				user: "anotheruser",
				pass: "anotherpass",
			},
			want: []byte("\x00anotheruser\x00anotherpass"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, prepareAuthData(tt.args.user, tt.args.pass), "prepareAuthData(%v, %v)", tt.args.user, tt.args.pass)
		})
	}
}

func TestOpCode_changeOnQuiet(t *testing.T) {
	type args struct {
		def OpCode
	}
	tests := []struct {
		name string
		o    OpCode
		args args
		want OpCode
	}{
		{
			name: GET.String(),
			o:    GET,
			args: args{def: GETQ},
			want: GETQ,
		},
		{
			name: GETQ.String(),
			o:    GETQ,
			args: args{def: GETQ},
			want: GETQ,
		},
		{
			name: "unknown opcode",
			o:    OpCode(0x1b),
			args: args{def: GETQ},
			want: GETQ,
		},
		{
			name: SET.String(),
			o:    SET,
			args: args{def: GETQ},
			want: SETQ,
		},
		{
			name: ADD.String(),
			o:    ADD,
			args: args{def: GETQ},
			want: ADDQ,
		},
		{
			name: REPLACE.String(),
			o:    REPLACE,
			args: args{def: GETQ},
			want: REPLACEQ,
		},
		{
			name: DELETE.String(),
			o:    DELETE,
			args: args{def: GETQ},
			want: DELETEQ,
		},
		{
			name: INCREMENT.String(),
			o:    INCREMENT,
			args: args{def: GETQ},
			want: INCREMENTQ,
		},
		{
			name: DECREMENT.String(),
			o:    DECREMENT,
			args: args{def: GETQ},
			want: DECREMENTQ,
		},
		{
			name: FLUSH.String(),
			o:    FLUSH,
			args: args{def: GETQ},
			want: FLUSHQ,
		},
		{
			name: APPEND.String(),
			o:    APPEND,
			args: args{def: GETQ},
			want: APPENDQ,
		},
		{
			name: PREPEND.String(),
			o:    PREPEND,
			args: args{def: GETQ},
			want: PREPENDQ,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.o.changeOnQuiet(tt.args.def), "changeOnQuiet(%v)", tt.args.def)
		})
	}
}
