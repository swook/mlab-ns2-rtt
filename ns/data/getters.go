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
)

var (
	ErrNoMatchingSliverTool = errors.New("No matching SliverTool found.")
	ErrNoMatchingSite       = errors.New("No matching Site found.")
)

// GetSliverTools returns a list of all SliverTools.
func GetSliverTools(c appengine.Context) ([]*SliverTool, error) {
	q := datastore.NewQuery("SliverTool")
	var slivers []*SliverTool
	if err := QueryData(c, "SliverTools", q, &slivers); err != nil {
		return nil, err
	}
	return slivers, nil
}

// GetSliverToolsWithToolID returns a list of SliverTools which run an M-Lab
// tool with ID, toolID.
func GetSliverToolsWithToolID(c appengine.Context, toolID string) ([]*SliverTool, error) {
	q := datastore.NewQuery("SliverTool").Filter("tool_id =", toolID)
	var slivers []*SliverTool
	if err := QueryData(c, toolID, q, &slivers); err != nil {
		return nil, err
	}
	return slivers, nil
}

// GetRandomSliverFromSite returns a randomly selected online SliverTool from a
// list of SliverTools which run an M-Lab tool with ID toolID on an M-Lab site
// with ID siteID.
func GetRandomSliverFromSite(c appengine.Context, toolID, siteID string) (*SliverTool, error) {
	slivers, err := GetSliverToolsWithToolID(c, toolID)
	if err != nil {
		return nil, err
	}

	slivers = FilterOnline(slivers)                     // Filter out offline slivers
	siteslivers := make([]*SliverTool, 0, len(slivers)) // Get Slivers of required Site ID
	for _, s := range slivers {
		if s.SiteID == siteID {
			siteslivers = append(siteslivers, s)
		}
	}

	if len(siteslivers) == 0 {
		return nil, ErrNoMatchingSliverTool
	}

	idx := rand.Int() % len(siteslivers) // Get random index
	return siteslivers[idx], nil
}

// GetSiteWithSiteID returns a Site which matches a provided site ID.
func GetSiteWithSiteID(c appengine.Context, siteID string) (*Site, error) {
	q := datastore.NewQuery("Site").Filter("site_id =", siteID)
	var sites []*Site
	if err := QueryData(c, siteID, q, &sites); err != nil {
		return nil, err
	}
	if len(sites) == 0 {
		return nil, ErrNoMatchingSite
	}
	return sites[0], nil
}

// GetAllSites returns an array of all the Sites in the datastore
func GetAllSites(c appengine.Context) ([]*Site, []*datastore.Key, error) {
	q := datastore.NewQuery("Sites")
	var sites []*Site
	sk, err := q.GetAll(c, sites)
	return sites, sk, err
}
