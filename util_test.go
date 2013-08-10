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

package rtt

import (
	"net"
	"testing"
)

var getClientGroupTests = []struct {
	in     string
	subnet string
}{
	{"173.194.36.73", "173.194.36.0"},
	{"173.194.39.255", "173.194.36.0"},
	{"2a03:2880:2110:df07:face:b00c:0:1", "2a03:2880:2110:df00::"},
	{"2a03:2880:2110:df11:b00c:face:0:1", "2a03:2880:2110:df00::"},
}

func TestGetClientGroupIPv4(t *testing.T) {
	var cg *net.IPNet
	for _, tt := range getClientGroupTests {
		cg = GetClientGroup(net.ParseIP(tt.in))
		if tt.subnet != cg.IP.String() {
			t.Fatalf("GetClientGroup(%v) = %v, want %v", tt.in, cg.IP, tt.subnet)
		}
	}
}

var isEqualClientGroupTests = []struct {
	a  string
	b  string
	ok bool
}{
	{"173.194.36.73", "173.194.39.255", true},
	{"173.194.35.73", "173.194.39.255", false},
	{"2a03:2880:2110:df07:face:b00c:0:1", "2a03:2880:2110:df11:b00c:face:0:1", true},
	{"2a03:2880:2110:ef07:face:b00c:0:1", "2a03:2880:2110:df11:b00c:face:0:1", false},
}

func TestIsEqualClientGroup(t *testing.T) {
	var ok bool
	for _, tt := range isEqualClientGroupTests {
		ok = IsEqualClientGroup(net.ParseIP(tt.a), net.ParseIP(tt.b))
		if ok != tt.ok {
			t.Fatalf("IsEqualClientGroup(%v, %v) = %v, want %v", tt.a, tt.b, ok, tt.ok)
		}
	}
}
