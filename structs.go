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

// RTTDB is a list of *ClientGroup which stores all RTT-related information
var RTTDB = make([]*ClientGroup, 0)

// ClientGroup contains an aggregation of RTT information by /v4PrefixSize or
// /v6PrefixSize
type ClientGroup struct {
	Prefix   net.IP
	SiteRTTs SiteRTTs
}

// getCGFromData gets a *ClientGroup from RTTDB where an input *net.IPNet equals
// the *ClientGroup.Prefix.
// When no entry exists, a new entry is made and returned.
func getCGFromData(ipnet *net.IPNet) *ClientGroup {
	for _, v := range RTTDB {
		if v.Prefix.Equal(ipnet.IP) {
			return v
		}
	}
	nCG := &ClientGroup{
		Prefix:   ipnet.IP,
		SiteRTTs: make([]*SiteRTT, 0),
	}
	RTTDB = append(RTTDB, nCG)
	return nCG
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
type SiteRTTs []*SiteRTT

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

// getSiteRTTFromCG gets a *SiteRTT from an input *ClientGroup where an input
// *Site matches an entry in the list of *SiteRTT
// When no entry exists, a new entry is made and returned.
func getSiteRTTFromCG(cgo *ClientGroup, site *Site) *SiteRTT {
	for _, v := range cgo.SiteRTTs {
		if SitesDB[v.SiteID] == site {
			return v
		}
	}
	nSiteRTT := &SiteRTT{
		SiteID: site.ID,
	}
	cgo.SiteRTTs = append(cgo.SiteRTTs, nSiteRTT)
	return nSiteRTT
}

// SitesDB stores a list of sites
var SitesDB = make(map[string]*Site, 0)

// A Site contains a set of Slivers
type Site struct {
	ID      string
	Slivers []*Sliver
}

// Sliver represents a server which runs within a parent Site
type Sliver struct {
	IP   net.IP
	Site *Site
}

// getSiteWithIP gets a *Site from SitesDB where a member *Sliver has an IP
// equal to input net.IP
func getSiteWithIP(ip net.IP) *Site {
	for _, site := range SitesDB {
		for _, sliver := range site.Slivers {
			if sliver.IP.Equal(ip) {
				return site
			}
		}
	}
	return nil
}
