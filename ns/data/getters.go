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

	ipStr := ip.String()
	isIPv4 := ip.To4() != nil
	for _, s := range slivers {
		if isIPv4 && s.SliverIPv4 == ipStr {
			return s, nil
		} else if !isIPv4 && s.SliverIPv6 == ipStr {
			return s, nil
		}
	}
	return nil, ErrNoMatchingSliverTool
}
