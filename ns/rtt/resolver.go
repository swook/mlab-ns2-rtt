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

package rtt

import (
	"appengine"
	"appengine/datastore"
	"code.google.com/p/mlab-ns2/gae/ns/data"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
)

const (
	URLRTTMain       = "/rtt/"
	RTTToolIDPattern = "^/rtt/([^/]+)"
)

var (
	ErrNoToolIDSpecified = errors.New("rtt: No Tool ID specified in request.")
	ErrNotEnoughData     = errors.New("rtt: The RTT resolver has insufficient data to respond to this query.")
	ErrInvalidSiteID     = errors.New("rtt: Invalid Site ID.")
	RTTToolIDRegexp, _   = regexp.Compile(RTTToolIDPattern)
)

func init() {
	http.HandleFunc(URLRTTMain, RTTHandler)
}

// RTTHandler is a simple handler which uses the URL parameter 'url' or client's
// IP to find a Sliver with lowest RTT.
func RTTHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	// Get IP to use as client IP.
	ipStr := r.FormValue("ip")
	if ipStr == "" {
		ipStr = r.RemoteAddr
	}
	ip := net.ParseIP(ipStr)

	// Get tool ID being queried for.
	toolIDMatch := RTTToolIDRegexp.FindStringSubmatch(r.URL.Path)
	if len(toolIDMatch) < 2 {
		fmt.Fprintln(w, ErrNoToolIDSpecified)
		return
	}
	toolID := toolIDMatch[1]

	// Query RTT resolver.
	resp, err := RTTResolver(c, toolID, ip)
	if err != nil {
		c.Errorf("rtt.RTTHandler: %s", err)
		fmt.Fprintln(w, err)
	} else {
		fmt.Fprintln(w, resp)
	}
}

// RTTResolver returns a Sliver from a Site with lowest RTT given a client's IP.
func RTTResolver(c appengine.Context, toolID string, ip net.IP) (net.IP, error) {
	cgIP := GetClientGroup(ip).IP
	rttKey := datastore.NewKey(c, "string", "rtt", 0, nil)
	key := datastore.NewKey(c, "ClientGroup", cgIP.String(), 0, rttKey)

	// Get ClientGroup from datastore.
	var cg ClientGroup
	err := data.GetData(c, mcClientGroupKey(c, cgIP), key, &cg)
	if err != nil {
		return nil, err
	}

	// Get first error-less Site and a random SliverTool from selected Site.
	var siteID string
	var sliverTool *data.SliverTool
	for _, sr := range cg.SiteRTTs {
		siteID = sr.SiteID
		sliverTool, err = data.GetRandomSliverFromSite(c, toolID, siteID)
		if err == nil {
			return net.ParseIP(sliverTool.SliverIPv4), nil
		}
	}
	// No valid Site found.
	return nil, ErrNotEnoughData
}

// mcClientGroupKey returns a key for use in memcache.
func mcClientGroupKey(c appengine.Context, ip net.IP) string {
	key := fmt.Sprintf("rtt:ClientGroup:%s", ip)
	return key
}
