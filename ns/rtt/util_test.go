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
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"
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

var mergeSiteRTTsTests = []struct {
	oldIn   *SiteRTT
	newIn   *SiteRTT
	out     *SiteRTT
	changed bool
}{
	// Case with lower RTT in new SiteRTT
	{
		&SiteRTT{"abc01", 1.1, time.Unix(1, 0)},
		&SiteRTT{"abc01", 0.1, time.Unix(1, 1)},
		&SiteRTT{"abc01", 0.1, time.Unix(1, 1)},
		true,
	},
	// Case with lower RTT in old SiteRTT
	{
		&SiteRTT{"abc01", 0.1, time.Unix(1, 0)},
		&SiteRTT{"abc01", 1.1, time.Unix(1, 1)},
		&SiteRTT{"abc01", 0.1, time.Unix(1, 0)},
		false,
	},
}

func TestMergeSiteRTTs(t *testing.T) {
	var ok bool
	var err error
	var newSiteRTTStr string
	for _, tt := range mergeSiteRTTsTests {
		newSiteRTTStr = fmt.Sprintf("%v", tt.oldIn)
		ok, err = MergeSiteRTTs(tt.oldIn, tt.newIn)
		if err != nil || !reflect.DeepEqual(tt.oldIn, tt.out) || ok != tt.changed {
			t.Fatalf("MergeSiteRTTs(%s, %v) = %v, %v, want %v, %v", newSiteRTTStr, tt.newIn, tt.oldIn, ok, tt.out, tt.changed)
		}
	}
}

var mergeClientGroupsTests = []struct {
	oldIn   *ClientGroup
	newIn   *ClientGroup
	out     *ClientGroup
	changed bool
}{
	// Case with new insert and update of old value
	{
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 1.1, time.Unix(1, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.9, time.Unix(3, 0)},
			SiteRTT{"def01", 4.2, time.Unix(2, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.9, time.Unix(3, 0)},
			SiteRTT{"def01", 4.2, time.Unix(2, 0)},
		}},
		true,
	},
	// Case with new insert only
	{
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.9, time.Unix(3, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"def01", 4.2, time.Unix(2, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.9, time.Unix(3, 0)},
			SiteRTT{"def01", 4.2, time.Unix(2, 0)},
		}},
		true,
	},
	// Update two old values
	{
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.9, time.Unix(3, 0)},
			SiteRTT{"def01", 4.2, time.Unix(2, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.7, time.Unix(4, 0)},
			SiteRTT{"def01", 4.0, time.Unix(5, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.7, time.Unix(4, 0)},
			SiteRTT{"def01", 4.0, time.Unix(5, 0)},
		}},
		true,
	},
	// No change
	{
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.7, time.Unix(4, 0)},
			SiteRTT{"def01", 4.0, time.Unix(5, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.9, time.Unix(3, 0)},
			SiteRTT{"def01", 4.2, time.Unix(2, 0)},
		}},
		&ClientGroup{[]byte{173, 194, 36, 73}, []SiteRTT{
			SiteRTT{"abc01", 0.7, time.Unix(4, 0)},
			SiteRTT{"def01", 4.0, time.Unix(5, 0)},
		}},
		false,
	},
}

func TestMergeClientGroups(t *testing.T) {
	var ok bool
	var err error
	var newCGStr string
	for _, tt := range mergeClientGroupsTests {
		newCGStr = fmt.Sprintf("%v", tt.oldIn)
		ok, err = MergeClientGroups(tt.oldIn, tt.newIn)
		if err != nil || !reflect.DeepEqual(tt.oldIn, tt.out) || ok != tt.changed {
			t.Fatalf("MergeClientGroups(%s, %v) = %v, %v, want %v, %v", newCGStr, tt.newIn, tt.oldIn, ok, tt.out, tt.changed)
		}
	}
}
