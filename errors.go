package writeaheadlog

import (
	"errors"
	"strings"
)

// composeErrors composes multiple errors into a single error. Any nil errors
// are omitted. If all of the errors are nil, composeErrors returns nil.
func composeErrors(errs ...error) error {
	// Strip out any nil errors.
	var errStrings []string
	for _, err := range errs {
		if err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}
	// Return nil if there are no non-nil errors.
	if len(errStrings) == 0 {
		return nil
	}
	return errors.New(strings.Join(errStrings, "; "))
}

// extendErr returns a new error concatenating msg with the original error
// message. If err is nil, extendErr returns nil.
func extendErr(msg string, err error) error {
	if err == nil {
		return nil
	}
	return errors.New(msg + ": " + err.Error())
}
