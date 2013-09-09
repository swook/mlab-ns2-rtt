// Copyright 2013 M-Lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build appengine

package data

import (
	"fmt"
	"net"
)

// MCKey_ClientGroup returns a key for use in memcache for rtt.ClientGroup data.
func MCKey_ClientGroup(ip net.IP) string {
	key := fmt.Sprintf("rtt:ClientGroup:%s", ip)
	return key
}
