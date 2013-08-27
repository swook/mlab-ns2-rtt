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
	"fmt"
	"math/rand"
	"net"
)

var (
	ErrNoMatchingSliverTool = errors.New("No matching SliverTool found.")
)

// GetSliverTools returns a list of all SliverTools.
func GetSliverTools(c appengine.Context) ([]*SliverTool, error) {
	q := datastore.NewQuery("SliverTool")
	var slivers []*SliverTool
	if err := QueryData(c, "SliverTools", q, slivers); err != nil {
		return nil, err
	}
	return slivers, nil
}

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

// GetRandomSliverFromSite returns a randomly selected SliverTool from a list of
// SliverTools which run an M-Lab tool with ID toolID on an M-Lab site with ID
// siteID.
func GetRandomSliverFromSite(c appengine.Context, toolID, siteID string) (*SliverTool, error) {
	q := datastore.NewQuery("SliverTool").Filter("tool_id =", toolID).Filter("site_id =", siteID)
	var slivers []*SliverTool
	key := fmt.Sprintf("%s:%s", toolID, siteID)
	if err := QueryData(c, key, q, slivers); err != nil {
		return nil, err
	}
	slivers = FilterOnline(slivers)  // Filter out offline slivers
	idx := rand.Int() % len(slivers) // Get random index
	return slivers[idx], nil
}

// GetSiteWithSiteID returns a Site which matches a provided site ID.
func GetSiteWithSiteID(c appengine.Context, siteID string) (*Site, error) {
	k := datastore.NewKey(c, "Site", siteID, 0, nil)
	var site *Site
	if err := GetData(c, siteID, k, site); err != nil {
		return nil, err
	}
	return site, nil
}

// GetSliverToolWithIP returns a SliverTool which matches a provided IP.
func GetSliverToolWithIP(c appengine.Context, ip net.IP) (*SliverTool, error) {
	slivers, err := GetSliverTools(c)
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
