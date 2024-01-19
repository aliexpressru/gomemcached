package memcached

import (
	"errors"
	"fmt"
)

const libPrefix = "gomemcached"

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("gomemcached: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("gomemcached: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("gomemcached: item not stored")

	// ErrServerError means that a server error occurred.
	ErrServerError = errors.New("gomemcached: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("gomemcached: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("gomemcached: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("gomemcached: no servers configured or available")

	// ErrInvalidAddr means that an incorrect address was passed and could not be cast to net.Addr
	ErrInvalidAddr = errors.New("gomemcached: invalid address for server")

	// ErrServerNotAvailable means that one of the nodes is currently unavailable
	ErrServerNotAvailable = errors.New("gomemcached: server(s) is not available")

	// ErrNotConfigured means that some required parameter is not set in the configuration
	ErrNotConfigured = errors.New("gomemcached: not complete configuration")

	// ErrUnknownCommand means that in request consumer use unknown command for memcached.
	ErrUnknownCommand = errors.New("gomemcached: Unknown command")

	// ErrDataSizeExceedsLimit means that memcached cannot process the request data due to its size.
	ErrDataSizeExceedsLimit = errors.New("gomemcached: Data size exceeds limit")

	// ErrInvalidArguments indicates invalid arguments or operation parameters (non-user request error).
	ErrInvalidArguments = errors.New("gomemcached: Invalid arguments or operation parameters")

	// ErrAuthFail indicates that an authorization attempt was made, but it did not work
	ErrAuthFail = errors.New("gomemcached: authentication enabled but operation failed")
)

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch {
	case errors.Is(err, ErrCacheMiss), errors.Is(err, ErrCASConflict),
		errors.Is(err, ErrNotStored), errors.Is(err, ErrMalformedKey):
		return true
	}
	return false
}

func wrapMemcachedResp(resp *Response) error {
	switch resp.Status {
	case SUCCESS:
		return nil
	case KEY_ENOENT:
		return fmt.Errorf("%w. %w", ErrCacheMiss, resp)
	case NOT_STORED, KEY_EEXISTS:
		return fmt.Errorf("%w. %w", ErrNotStored, resp)
	case EINVAL, DELTA_BADVAL:
		return fmt.Errorf("%w. %w", ErrInvalidArguments, resp)
	case ENOMEM:
		return fmt.Errorf("%w. %w", ErrServerError, resp)
	case TMPFAIL:
		return fmt.Errorf("%w. %w", ErrServerNotAvailable, resp)
	case UNKNOWN_COMMAND:
		return fmt.Errorf("%w. %w", ErrUnknownCommand, resp)
	case E2BIG:
		return fmt.Errorf("%w. %w", ErrDataSizeExceedsLimit, resp)
	default:
		return fmt.Errorf("%w. %w", ErrServerError, resp)
	}
}

func errStatus(e error) Status {
	status := UNKNOWN_STATUS
	var res *Response
	if errors.As(e, &res) {
		status = res.Status
	}
	return status
}
