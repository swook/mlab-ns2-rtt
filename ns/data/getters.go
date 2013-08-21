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
	ErrNoMatchingSliverTool = errors.New("")
)

func GetSliverToolsWithToolID(c appengine.Context, toolID string) ([]*SliverTool, error) {
	q := datastore.NewQuery("SliverTool").Filter("tool_id =", toolID)
	var slivers []*SliverTool
	if err := QueryData(c, toolID, q, slivers); err != nil {
		return nil, err
	}
	return slivers, nil
}

func GetRandomSliverToolWithToolID(c appengine.Context, toolID string) (*SliverTool, error) {
	slivers, err := GetSliverToolsWithToolID(c, toolID)
	if err != nil {
		return nil, err
	}
	idx := rand.Int() % len(slivers)
	return slivers[idx], nil
}

func GetSiteWithSiteID(c appengine.Context, siteID string) (*Site, error) {
	q := datastore.NewQuery("Site").Filter("site_id =", siteID)
	var site *Site
	if err := QueryData(c, siteID, q, site); err != nil {
		return nil, err
	}
	return site, nil
}

func GetSliverToolWithIP(c appengine.Context, toolID string, ip net.IP) (*SliverTool, error) {
	slivers, err := GetSliverToolsWithToolID(c, toolID)
	if err != nil {
		return nil, err
	}
	for _, s := range slivers {
		if net.IP(s.SliverIPv4).Equal(ip) || net.IP(s.SliverIPv6).Equal(ip) {
			return s, nil
		}
	}
	return nil, ErrNoMatchingSliverTool
}
