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
	"reflect"
	"testing"
)

var siteRTTsLessTests = []struct {
	in   SiteRTTs
	idx1 int
	idx2 int
	out  bool
}{
	{
		SiteRTTs{
			SiteRTT{RTT: 0.1},
			SiteRTT{RTT: 10.3},
		},
		0,
		1,
		true,
	},
	{
		SiteRTTs{
			SiteRTT{RTT: 21.5},
			SiteRTT{RTT: 2.7},
		},
		0,
		1,
		false,
	},
}

func TestSiteRTTsLess(t *testing.T) {
	var out bool
	for _, tt := range siteRTTsLessTests {
		out = tt.in.Less(tt.idx1, tt.idx2)
		if out != tt.out {
			t.Fatalf("SiteRTTs.Less(%v, %v) = %v, want %v", tt.idx1, tt.idx2, out, tt.out)
		}
	}
}

var siteRTTsSwapTests = []struct {
	in   SiteRTTs
	idx1 int
	idx2 int
	out  SiteRTTs
}{
	{
		SiteRTTs{
			SiteRTT{RTT: 0.1},
			SiteRTT{RTT: 21.5},
			SiteRTT{RTT: 2.7},
			SiteRTT{RTT: 10.3},
		},
		1,
		3,
		SiteRTTs{
			SiteRTT{RTT: 0.1},
			SiteRTT{RTT: 10.3},
			SiteRTT{RTT: 2.7},
			SiteRTT{RTT: 21.5},
		},
	},
}

func TestSiteRTTsSwap(t *testing.T) {
	for _, tt := range siteRTTsSwapTests {
		tt.in.Swap(tt.idx1, tt.idx2)
		if !reflect.DeepEqual(tt.in, tt.out) {
			t.Fatalf("SiteRTTs.Less(%v, %v) = %v, want %v", tt.idx1, tt.idx2, tt.in, tt.out)
		}
	}
}

var siteRTTsLenTests = []struct {
	in  SiteRTTs
	out int
}{
	{
		SiteRTTs{},
		0,
	},
	{
		SiteRTTs{
			SiteRTT{},
			SiteRTT{},
		},
		2,
	},
}

func TestSiteRTTsLen(t *testing.T) {
	var out int
	for _, tt := range siteRTTsLenTests {
		out = tt.in.Len()
		if out != tt.out {
			t.Fatalf("SiteRTTs.Len() = %v, want %v", out, tt.out)
		}
	}
}
