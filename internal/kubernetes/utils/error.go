package utils

import (
	"errors"
	"strings"
)

func JoinErrors(errs []error, seperator string) error {
	if len(errs) == 0 {
		return nil
	}

	reasons := make([]string, 0, len(errs))
	for _, err := range errs {
		reasons = append(reasons, err.Error())
	}

	return errors.New(strings.Join(reasons, seperator))
}
