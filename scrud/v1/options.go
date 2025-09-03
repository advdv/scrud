package scrudv1

import (
	"fmt"
	"strconv"
	"strings"
)

// UnmarshalText parses a symbolic name (or a numeric literal) into the enum.
func (k *ActionKind) UnmarshalText(text []byte) error {
	s := strings.ToUpper(strings.TrimSpace(string(text)))

	// Allow either the symbolic name or the numeric value.
	if v, ok := ActionKind_value[s]; ok {
		*k = ActionKind(v)
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 32); err == nil {
		*k = ActionKind(n)
		return nil
	}

	return fmt.Errorf("scrudv1: unknown ActionKind %q", s)
}
