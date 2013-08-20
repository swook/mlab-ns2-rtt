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
	"time"
)

// ClientGroup contains an aggregation of RTT information by /v4PrefixSize or
// /v6PrefixSize
type ClientGroup struct {
	Prefix   []byte
	SiteRTTs SiteRTTs
}

// NewClientGroup returns a new *ClientGroup with a Prefix set using a provided
// IP.
func NewClientGroup(ip net.IP) *ClientGroup {
	return &ClientGroup{
		Prefix:   []byte(ip),
		SiteRTTs: make(SiteRTTs, 0),
	}
}

// SiteRTT contains information of a ClientGroup's aggregated RTT to a Site.
// NOTE: RTT is assumed to be bi-directionally equal between nodes. This is not
// necessarily so.
type SiteRTT struct {
	SiteID      string
	RTT         float64
	LastUpdated time.Time
}

// SiteRTTs is a list of RTT data from ClientGroup to Site
type SiteRTTs []SiteRTT

// Less allows for the sorting of SiteRTTs in a *ClientGroup
func (l SiteRTTs) Less(i, j int) bool {
	return l[i].RTT <= l[j].RTT
}

// Swap allows for the sorting of SiteRTTs in a *ClientGroup
func (l SiteRTTs) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// Len allows for the sorting of SiteRTTs in a *ClientGroup
func (l SiteRTTs) Len() int {
	return len(l)
}

// SitesDB stores a map of site IDs to *Sites.
var SitesDB = make(map[string]*Site)

// A Site contains a set of Slivers
type Site struct {
	ID      string
	Slivers []*Sliver
}

// SliversDB stores a map of sliver IPs to *Sites.
var SliversDB = make(map[string]*Site)

// Sliver represents a server which runs within a parent Site
type Sliver struct {
	IP   net.IP
	Site *Site
}
