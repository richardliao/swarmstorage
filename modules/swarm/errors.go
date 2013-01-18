package swarm

import (
	"fmt"
)

type SwarmError struct {
	Code    int
	Message string
}

func (this SwarmError) Error() string {
	return fmt.Sprintf("at %d, %s", this.Code, this.Message)
}

// Formats according to a format specifier and returns the string as a value 
// that satisfies error.
func SwarmErrorf(code int, format string, a ...interface{}) *SwarmError {
	return &SwarmError{code, fmt.Sprintf(format, a...)}
}
