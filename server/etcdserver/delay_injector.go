// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdserver

import (
	"os"
	"strconv"
	"time"
)

var (
	causalDelayTarget = os.Getenv("ETCD_CAUSAL_DELAY_TARGET")
	causalDelayMs, _  = strconv.Atoi(os.Getenv("ETCD_CAUSAL_DELAY_MS"))
)

// InjectCausalDelay blocks execution if target matches ETCD_CAUSAL_DELAY_TARGET.
func InjectCausalDelay(target string) {
	if causalDelayMs <= 0 || causalDelayTarget != target {
		return
	}
	time.Sleep(time.Duration(causalDelayMs) * time.Millisecond)
}
