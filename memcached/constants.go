// Package memcached binary protocol packet formats and constants.
package memcached

import (
	"fmt"
)

const (
	REQ_MAGIC = 0x80
	RES_MAGIC = 0x81
)

const (
	SaslMechanism = "PLAIN"
)

type OpCode uint8

const (
	GET        = OpCode(0x00)
	SET        = OpCode(0x01)
	ADD        = OpCode(0x02)
	REPLACE    = OpCode(0x03)
	DELETE     = OpCode(0x04)
	INCREMENT  = OpCode(0x05)
	DECREMENT  = OpCode(0x06)
	QUIT       = OpCode(0x07)
	FLUSH      = OpCode(0x08)
	GETQ       = OpCode(0x09)
	NOOP       = OpCode(0x0a)
	VERSION    = OpCode(0x0b)
	GETK       = OpCode(0x0c)
	GETKQ      = OpCode(0x0d)
	APPEND     = OpCode(0x0e)
	PREPEND    = OpCode(0x0f)
	STAT       = OpCode(0x10)
	SETQ       = OpCode(0x11)
	ADDQ       = OpCode(0x12)
	REPLACEQ   = OpCode(0x13)
	DELETEQ    = OpCode(0x14)
	INCREMENTQ = OpCode(0x15)
	DECREMENTQ = OpCode(0x16)
	QUITQ      = OpCode(0x17)
	FLUSHQ     = OpCode(0x18)
	APPENDQ    = OpCode(0x19)
	PREPENDQ   = OpCode(0x1a)

	SASL_LIST_MECHS = OpCode(0x20)
	SASL_AUTH       = OpCode(0x21)
	SASL_STEP       = OpCode(0x22)
)

type Status uint16

const (
	// SUCCESS - Successful operation.
	SUCCESS = Status(0x00)
	// KEY_ENOENT - Key not found.
	KEY_ENOENT = Status(0x01)
	// KEY_EEXISTS -Key already exists.
	KEY_EEXISTS = Status(0x02)
	// E2BIG - Data size exceeds limit.
	E2BIG = Status(0x03)
	// EINVAL - Invalid arguments or operation parameters.
	EINVAL = Status(0x04)
	// NOT_STORED - Operation was not performed because the data was not stored.
	NOT_STORED = Status(0x05)
	// DELTA_BADVAL - Invalid value specified for increment/decrement.
	DELTA_BADVAL = Status(0x06)
	// AUTHFAIL - Authentication required / Not Successful.
	AUTHFAIL = Status(0x20)
	// FURTHER_AUTH - Further authentication steps required.
	FURTHER_AUTH = Status(0x21)
	// UNKNOWN_COMMAND - Unknown command.
	UNKNOWN_COMMAND = Status(0x81)
	// ENOMEM - Insufficient memory for the operation.
	ENOMEM = Status(0x82)
	// TMPFAIL - Temporary failure, the operation cannot be performed at the moment.
	TMPFAIL = Status(0x86)

	// UNKNOWN_STATUS is not a Memcached status
	UNKNOWN_STATUS = Status(0xffff)
)

const (
	// HDR_LEN is a number of bytes in a binary protocol header.
	HDR_LEN  = 24
	BODY_LEN = 128
)

// Mapping of OpCode -> name of command (not exhaustive)
var CommandNames map[OpCode]string

var StatusNames map[Status]string

// nolint:goconst
func init() {
	CommandNames = make(map[OpCode]string)
	CommandNames[GET] = "GET"
	CommandNames[SET] = "SET"
	CommandNames[ADD] = "ADD"
	CommandNames[REPLACE] = "REPLACE"
	CommandNames[DELETE] = "DELETE"
	CommandNames[INCREMENT] = "INCREMENT"
	CommandNames[DECREMENT] = "DECREMENT"
	CommandNames[QUIT] = "QUIT"
	CommandNames[FLUSH] = "FLUSH"
	CommandNames[GETQ] = "GETQ"
	CommandNames[NOOP] = "NOOP"
	CommandNames[VERSION] = "VERSION"
	CommandNames[GETK] = "GETK"
	CommandNames[GETKQ] = "GETKQ"
	CommandNames[APPEND] = "APPEND"
	CommandNames[PREPEND] = "PREPEND"
	CommandNames[STAT] = "STAT"
	CommandNames[SETQ] = "SETQ"
	CommandNames[ADDQ] = "ADDQ"
	CommandNames[REPLACEQ] = "REPLACEQ"
	CommandNames[DELETEQ] = "DELETEQ"
	CommandNames[INCREMENTQ] = "INCREMENTQ"
	CommandNames[DECREMENTQ] = "DECREMENTQ"
	CommandNames[QUITQ] = "QUITQ"
	CommandNames[FLUSHQ] = "FLUSHQ"
	CommandNames[APPENDQ] = "APPENDQ"
	CommandNames[PREPENDQ] = "PREPENDQ"

	CommandNames[SASL_LIST_MECHS] = "SASL_LIST_MECHS"
	CommandNames[SASL_AUTH] = "SASL_AUTH"
	CommandNames[SASL_STEP] = "SASL_STEP"

	StatusNames = make(map[Status]string)
	StatusNames[SUCCESS] = "SUCCESS"
	StatusNames[KEY_ENOENT] = "KEY_ENOENT"
	StatusNames[KEY_EEXISTS] = "KEY_EEXISTS"
	StatusNames[E2BIG] = "E2BIG"
	StatusNames[EINVAL] = "EINVAL"
	StatusNames[NOT_STORED] = "NOT_STORED"
	StatusNames[DELTA_BADVAL] = "DELTA_BADVAL"
	StatusNames[AUTHFAIL] = "AUTHFAIL"
	StatusNames[FURTHER_AUTH] = "FURTHER_AUTH"
	StatusNames[UNKNOWN_COMMAND] = "UNKNOWN_COMMAND"
	StatusNames[ENOMEM] = "ENOMEM"
	StatusNames[TMPFAIL] = "TMPFAIL"
}

// String an op code.
func (o OpCode) String() (rv string) {
	rv = CommandNames[o]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(o))
	}
	return rv
}

// String an op code.
func (s Status) String() (rv string) {
	rv = StatusNames[s]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(s))
	}
	return rv
}

// IsQuiet return true if a command is a "quiet" command.
func (o OpCode) IsQuiet() bool {
	switch o {
	case GETQ,
		GETKQ,
		SETQ,
		ADDQ,
		REPLACEQ,
		DELETEQ,
		INCREMENTQ,
		DECREMENTQ,
		QUITQ,
		FLUSHQ,
		APPENDQ,
		PREPENDQ:
		return true
	}
	return false
}

func (o OpCode) changeOnQuiet(def OpCode) OpCode {
	if o.IsQuiet() {
		return o
	}
	switch o {
	case GET:
		return GETQ
	case SET:
		return SETQ
	case ADD:
		return ADDQ
	case REPLACE:
		return REPLACEQ
	case DELETE:
		return DELETEQ
	case INCREMENT:
		return INCREMENTQ
	case DECREMENT:
		return DECREMENTQ
	case FLUSH:
		return FLUSHQ
	case APPEND:
		return APPENDQ
	case PREPEND:
		return PREPENDQ
	default:
		return def
	}
}

func prepareAuthData(user, pass string) []byte {
	return []byte(fmt.Sprintf("\x00%s\x00%s", user, pass))
}
