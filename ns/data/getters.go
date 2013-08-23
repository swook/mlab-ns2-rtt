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
	"appengine"
	"appengine/datastore"
	"errors"
	"math/rand"
	"net"
)

var (
	ErrNoMatchingSliverTool = errors.New("No matching SliverTool found.")
)

// GetSliverToolsWithToolID returns a list of SliverTools which run an M-Lab
// tool with ID, toolID.
func GetSliverToolsWithToolID(c appengine.Context, toolID string) ([]*SliverTool, error) {
	q := datastore.NewQuery("SliverTool").Filter("tool_id =", toolID)
	var slivers []*SliverTool
	if err := QueryData(c, toolID, q, slivers); err != nil {
		return nil, err
	}
	return slivers, nil
}

// GetRandomSliverToolWithToolID returns a randomly selected SliverTool from a
// list of SliverTools which run an M-Lab tool with ID, toolID.
func GetRandomSliverToolWithToolID(c appengine.Context, toolID string) (*SliverTool, error) {
	slivers, err := GetSliverToolsWithToolID(c, toolID)
	if err != nil {
		return nil, err
	}
	idx := rand.Int() % len(slivers)
	return slivers[idx], nil
}

// GetSiteWithSiteID returns a Site which matches a provided site ID.
func GetSiteWithSiteID(c appengine.Context, siteID string) (*Site, error) {
	q := datastore.NewQuery("Site").Filter("site_id =", siteID)
	var site *Site
	if err := QueryData(c, siteID, q, site); err != nil {
		return nil, err
	}
	return site, nil
}

// GetSliverToolWithIP returns a SliverTool which matches a provided IP.
func GetSliverToolWithIP(c appengine.Context, toolID string, ip net.IP) (*SliverTool, error) {
	slivers, err := GetSliverToolsWithToolID(c, toolID)
	if err != nil {
		return nil, err
	}

	var isIPv4 bool
	if ip.To4() != nil {
		isIPv4 = true
	}

	for _, s := range slivers {
		if isIPv4 {
			// ip is IPv4 address
			if net.IP(s.SliverIPv4).Equal(ip) {
				return s, nil
			}
		} else {
			// ip is IPv6 address
			if net.IP(s.SliverIPv6).Equal(ip) {
				return s, nil
			}
		}
	}
	return nil, ErrNoMatchingSliverTool
}
