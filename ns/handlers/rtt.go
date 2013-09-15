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

package handlers

import (
	"appengine"
	"appengine/datastore"
	"code.google.com/p/mlab-ns2/gae/ns/data"
	"code.google.com/p/mlab-ns2/gae/ns/rtt"
	"errors"
	"fmt"
	"net"
	"net/http"
)

const URLRTTMain = "/rtt/"

var (
	ErrNoToolIDSpecified = errors.New("rtt: No Tool ID specified in request.")
	ErrNotEnoughData     = errors.New("rtt: The RTT resolver has insufficient data to respond to this query.")
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

	// Get Tool ID
	toolID := r.FormValue("tool_id")
	if toolID == "" {
		http.Error(w, ErrNoToolIDSpecified.Error(), http.StatusInternalServerError)
		c.Errorf("rtt.RTTHandler: %s", ErrNoToolIDSpecified)
		return
	}

	// Query RTT resolver.
	resp, err := RTTResolver(c, toolID, ip)
	switch err {
	case ErrNotEnoughData:
		http.Error(w, err.Error(), http.StatusNotFound)
		c.Errorf("rtt.RTTHandler: %s", err)
	case nil:
		fmt.Fprintln(w, resp)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("rtt.RTTHandler: %s", err)
	}
}

// RTTResolver returns a Sliver from a Site with lowest RTT given a client's IP.
func RTTResolver(c appengine.Context, toolID string, ip net.IP) (net.IP, error) {
	cgIP := rtt.GetClientGroup(ip).IP
	rttKey := datastore.NewKey(c, "string", "rtt", 0, nil)
	key := datastore.NewKey(c, "ClientGroup", cgIP.String(), 0, rttKey)

	// Get ClientGroup from datastore.
	var cg rtt.ClientGroup
	err := data.GetData(c, MCKey_ClientGroup(cgIP), key, &cg)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			return nil, ErrNotEnoughData
		}
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

// MCKey_ClientGroup returns a key for use in memcache for rtt.ClientGroup data.
func MCKey_ClientGroup(ip net.IP) string {
	key := fmt.Sprintf("rtt:ClientGroup:%s", ip)
	return key
}
