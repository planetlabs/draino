/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultMaxNotReadyNodesPeriod = 60 * time.Second
	DefaultMaxPendingPodsPeriod   = 60 * time.Second
)

func ParseMaxInParameter(param string) (max int, isPercent bool, err error) {
	percent := strings.HasSuffix(param, "%")
	max, err = strconv.Atoi(strings.TrimSuffix(param, "%"))
	if err != nil {
		return -1, percent, errors.New("can't Parse argument for limiter")
	}
	return max, percent, nil
}
