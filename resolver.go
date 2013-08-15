// +build appengine

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
	"appengine"
	"appengine/datastore"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
)

const (
	URLRTTMain = "/rtt"
)

var (
	ErrNotEnoughData = errors.New("rtt: The RTT resolver has insufficient data to respond to this query.")
	ErrInvalidSiteID = errors.New("rtt: Invalid Site ID.")
)

func init() {
	http.HandleFunc(URLRTTMain, RTTHandler)
}

func RTTHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	ipStr := r.FormValue("ip")
	if ipStr == "" {
		ipStr = r.RemoteAddr
	}
	ip := net.ParseIP(ipStr)

	resp, err := RTTResolver(c, ip)
	if err != nil {
		c.Errorf("rtt.RTTHandler: %s", err)
		fmt.Fprintln(w, err)
	} else {
		fmt.Fprintln(w, resp)
	}
}

func RTTResolver(c appengine.Context, ip net.IP) (net.IP, error) {
	cg, err := DSGetClientGroup(c, ip)
	if err != nil {
		switch err {
		case datastore.ErrNoSuchEntity:
			return nil, ErrNotEnoughData
		default:
			return nil, err
		}
	}

	if len(cg.SiteRTTs) > 0 {
		siteID := cg.SiteRTTs[0].SiteID
		serverIP, err := PickRandomServerFromSite(siteID)
		return serverIP, err
	} else {
		return nil, ErrNotEnoughData
	}
	return nil, nil
}

func PickRandomServerFromSite(siteID string) (net.IP, error) {
	site, ok := SitesDB[siteID]
	if !ok {
		return nil, ErrInvalidSiteID
	}
	idx := rand.Int() % len(site.Slivers)
	return site.Slivers[idx].IP, nil
}
