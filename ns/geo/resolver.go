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

// The geo package provides the geo resolver
package geo

import (
	"appengine"
	"appengine/datastore"
	"container/list"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"code.google.com/p/iptrie/locmap"
	"code.google.com/p/mlab-ns2/gae/ns/data"
)

var (
	LMapIPv4 = locmap.NewLocationMap()
	//TODO: LMapIPv6 = locmap.NewLocationMap()
)

const (
	initIPv4LocationMapUrl = "/admin/initIPv4LocationMap"
	geoUrl                 = "/geo"
)

func init() {
	http.HandleFunc(geoUrl, geo)
	http.HandleFunc(initIPv4LocationMapUrl, initIPv4LocationMap)
	//TODO: http.HandleFunc("/initIPv6LocationMap", initIPv6LocationMap)
}

func initIPv4LMap(c appengine.Context) error {
	q := datastore.NewQuery("SliverTool").Filter("status_ipv4 =", "online")
	list := list.New()
	var sliverTools []*data.SliverTool
	_, err := q.GetAll(c, &sliverTools)
	if err != nil {
		return err
	}
	for _, sl := range sliverTools {
		data := &locmap.Data{
			Status:     true,
			ResourceId: sl.ToolID,
			ServerId:   sl.SliverIPv4,
			Lat:        sl.Latitude,
			Lon:        sl.Longitude,
		}
		list.PushBack(data)
	}
	LMapIPv4.UpdateMulti(list, nil)
	return nil
}

// initIPv4LocationMap initializes a locationMap with ipv4 ipaddress
func initIPv4LocationMap(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	err := initIPv4LMap(c)
	if err != nil {
		c.Errorf("initIPv4LocationMap:initIPv4LMap err = %v", err)
		//TODO: Retry until successful
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// geo returns the ipaddress of the closest sliverTool
func geo(w http.ResponseWriter, r *http.Request) {

	c := appengine.NewContext(r)
	toolID := r.FormValue("tool")
	if toolID == "" {
		toolID = "ndt"
	}
	header := r.Header
	latLon := header.Get("X-AppEngine-CityLatLong")

	var lat, lon float64
	if latLon == "" {
		lat, lon = getLatLon(c, r.RemoteAddr)
	} else {
		p := strings.Split(latLon, ",")
		lat, _ = strconv.ParseFloat(p[0], 64)
		lon, _ = strconv.ParseFloat(p[1], 64)
	}

	if LMapIPv4 == nil {
		//TODO: Resolve query using datastore
		c.Errorf("geo:LMapIPv4 is nil")
		//TODO: Add initIPv4LocationMap to task queue
		return
	}

	ipRes, err := LMapIPv4.GetServer(lat, lon, toolID)
	if err != nil {
		c.Errorf("geo:LMapIPv4.GetServer err = %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "TooldID:%s, Response:%s", toolID, ipRes)
}
