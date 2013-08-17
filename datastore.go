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
	"appengine/memcache"
	"fmt"
	"net"
)

func DSGetClientGroup(c appengine.Context, ip net.IP) (*ClientGroup, error) {
	cgIP := GetClientGroup(ip).IP

	var cg *ClientGroup
	cg, err := mcGetClientGroup(c, cgIP)
	switch err {
	case memcache.ErrCacheMiss:
		rttKey := datastore.NewKey(c, "string", "rtt", 0, nil)
		key := datastore.NewKey(c, "ClientGroup", cgIP.String(), 0, rttKey)

		var cgList []ClientGroup
		cgList = make([]ClientGroup, 1)
		if err := datastore.GetMulti(c, []*datastore.Key{key}, cgList); err != nil {
			return nil, err
		}
		cg = &cgList[0]
		mcSetClientGroup(c, cg)
	case nil:
		return cg, nil
	default:
		return nil, err
	}
	return cg, nil
}

func mcClientGroupKey(c appengine.Context, ip net.IP) string {
	key := fmt.Sprintf("rtt:ClientGroup:%s", ip)
	return key
}

func mcGetClientGroup(c appengine.Context, ip net.IP) (*ClientGroup, error) {
	var cg ClientGroup
	key := mcClientGroupKey(c, ip)
	_, err := memcache.JSON.Get(c, key, &cg)
	if err != nil {
		return nil, err
	}
	return &cg, nil
}

func mcSetClientGroup(c appengine.Context, cg *ClientGroup) error {
	item := &memcache.Item{
		Key:    mcClientGroupKey(c, net.IP(cg.Prefix)),
		Object: *cg,
	}
	err := memcache.JSON.Set(c, item)
	if err != nil {
		return err
	}
	return nil
}
