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

package geo

import (
	"appengine"
	"appengine/datastore"
	"errors"
	"net"

	"code.google.com/p/mlab-ns2/gae/ns/data"
)

var (
	ErrGeoLocationNotFound = errors.New("Geolocation not found in MaxMind datastore")
)

func ipToint64(ip net.IP) int64 {
	a := ip.To16()
	i := int64(a[12]) << uint(24)
	i += int64(a[13]) << uint(16)
	i += int64(a[14]) << uint(8)
	i += int64(a[15])
	return i
}

// getLatLon returns the geolocation from maxmind data given the ipaddress
func getLatLon(c appengine.Context, ipReq string) (float64, float64) {
	ip := net.ParseIP(ipReq)
	ipNum := ipToint64(ip)
	// The following will return only one entry from MMLocation
	q := datastore.NewQuery("MMLocation").Filter("RangeStart <", ipNum).Order("-RangeStart").Limit(1)
	var mmLoc []*data.MMLocation
	_, err := q.GetAll(c, &mmLoc)
	if err != nil {
		c.Errorf("getLatLon:q.GetAll(..mmLoc) err = %v", err)
		return 0, 0
	}

	if ipNum > mmLoc[0].RangeEnd {
		c.Errorf("getLatLon: err %v", ErrGeoLocationNotFound)
		return 0, 0
	}
	return float64(mmLoc[0].Latitude), float64(mmLoc[0].Longitude)
}
