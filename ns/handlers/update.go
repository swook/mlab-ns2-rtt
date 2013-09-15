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

// The handlers package provides the mlab-ns2 handlers"
package handlers

import (
	"appengine"
	"appengine/datastore"
	"appengine/urlfetch"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"code.google.com/p/mlab-ns2/gae/ns/data"
)

const (
	KsSitesUrl               = "http://ks.measurementlab.net/mlab-site-stats.json"
	KsRegistrationHandlerUrl = "/admin/KsRegistrationHandler"
)

var (
	serverIDs        = [...]string{"mlab1", "mlab2", "mlab3"}
	numServers       = len(serverIDs)
	ErrInvalidKsSite = errors.New("Invalid data in Site from Ks")
)

func init() {
	http.HandleFunc(KsRegistrationHandlerUrl, KsRegistrationHandler)
}

// getAllKsSites returns a list of all sites from ks
func getAllKsSites(c appengine.Context) ([]*data.Site, error) {
	var ksSites []*data.Site
	client := urlfetch.Client(c)
	res, err := client.Get(KsSitesUrl)
	if err != nil {
		return nil, err
	}

	jsonBlob, _ := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	err = json.Unmarshal(jsonBlob, &ksSites)
	return ksSites, nil
}

// validateSites checks if required site information is available
func validateSite(site *data.Site) bool {
	// required fields: SiteID, Latitude, Longitude, Country, Metro
	if site.SiteID == "" || site.Latitude == 0 || site.Longitude == 0 || site.Country == "" || len(site.Metro) == 0 {
		return false
	}
	return true
}

// difference(a, b) returns the values in b that are not in a
func difference(mlabSiteIds, ksSiteIds map[string]int) map[string]int {
	if mlabSiteIds == nil {
		return ksSiteIds
	}
	newSiteIds := make(map[string]int)
	for ksSiteId := range ksSiteIds {
		_, ok := mlabSiteIds[ksSiteId]
		if !ok {
			newSiteIds[ksSiteId] = 1
		}
	}
	return newSiteIds
}

// registerSite puts a Site and corresponding SliverTools in the datastore
func registerSite(c appengine.Context, site *data.Site) ([]*datastore.Key, error) {

	key := datastore.NewKey(c, "Site", site.SiteID, 0, nil)
	site.When = time.Now()
	_, err := datastore.Put(c, key, site)
	if err != nil {
		return nil, err
	}

	q := datastore.NewQuery("Tool")
	var tools []*data.Tool
	_, err = q.GetAll(c, &tools)
	if err != nil {
		return nil, err
	}

	sliverTools := make([]*data.SliverTool, len(tools)*numServers)
	slKeys := make([]*datastore.Key, len(tools)*numServers)
	i := 0
	for _, tool := range tools {
		for _, serverID := range serverIDs {
			sliverToolID := data.GetSliverToolID(tool.ToolID, tool.SliceID, serverID, site.SiteID)
			sliceParts := strings.Split(tool.SliceID, "_")
			sliverTool := &data.SliverTool{
				ToolID:                 tool.ToolID,
				SliceID:                tool.SliceID,
				SiteID:                 site.SiteID,
				ServerID:               serverID,
				FQDN:                   fmt.Sprintf("%s.%s.%s.%s.%s", sliceParts[1], sliceParts[0], serverID, site.SiteID, "measurement-lab.org"),
				ServerPort:             "",
				HTTPPort:               tool.HTTPPort,
				SliverIPv4:             "off",
				SliverIPv6:             "off",
				UpdateRequestTimestamp: site.RegistrationTimestamp,
				StatusIPv4:             "offline",
				StatusIPv6:             "offline",
				Latitude:               site.Latitude,
				Longitude:              site.Longitude,
				City:                   site.City,
				Country:                site.Country,
				When:                   time.Now(),
			}
			slKey := datastore.NewKey(c, "SliverTool", sliverToolID, 0, nil)
			slKeys[i] = slKey
			sliverTools[i] = sliverTool
			i++
		}
	}
	return datastore.PutMulti(c, slKeys, sliverTools)
}

// KsRegistrationHandler gets Site data from ks and updates the datastore
func KsRegistrationHandler(w http.ResponseWriter, r *http.Request) {

	c := appengine.NewContext(r)
	ksSites, err := getAllKsSites(c)
	if err != nil {
		c.Errorf("KsRegistrationHandler:getAllKsSites err = %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ksSiteIds := make(map[string]int)
	validSites := make([]*data.Site, len(ksSites))
	i := 0
	for _, site := range ksSites {
		if validateSite(site) {
			validSites[i] = site
			ksSiteIds[site.SiteID] = 1
			i++
		} else {
			c.Errorf("KsRegistrationHandler:validateSite err = %v", ErrInvalidKsSite)
		}
	}

	mlabSiteIds := make(map[string]int)
	mlabSites, _, err := data.GetAllSites(c)
	if err != nil {
		c.Errorf("KsRegistrationHandler:data.GetAllSites err = %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, mlabSite := range mlabSites {
		mlabSiteIds[mlabSite.SiteID] = 1
	}
	newSiteIds := difference(mlabSiteIds, ksSiteIds)

	for _, site := range validSites {
		if site == nil {
			break
		}
		_, ok := newSiteIds[site.SiteID]
		if ok {
			_, err = registerSite(c, site)
			if err != nil {
				c.Errorf("KsRegistrationHandler:registerSite err = %v", err)
			}
		}
	}
}
