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
	"net"
)

func DSGetClientGroup(c appengine.Context, ip net.IP) (*ClientGroup, error) {
	cgIP := GetClientGroup(ip)

	rttKey := datastore.NewKey(c, "string", "rtt", 0, nil)
	key := datastore.NewKey(c, "ClientGroup", cgIP.IP.String(), 0, rttKey)

	var cg []ClientGroup
	cg = make([]ClientGroup, 1)
	if err := datastore.GetMulti(c, []*datastore.Key{key}, cg); err != nil {
		return nil, err
	}
	return &cg[0], nil
}
