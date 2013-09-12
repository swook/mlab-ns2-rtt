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
	"code.google.com/p/mlab-ns2/gae/ns/rtt"
	"net/http"
	"time"
)

const (
	URLBQImportDaily = "/admin/rtt/import/daily"
	URLBQImportAll   = "/admin/rtt/import/all"
)

func init() {
	http.HandleFunc(URLBQImportDaily, bqImportDaily)
	http.HandleFunc(URLBQImportAll, bqImportAllTime)
}

// bqImportDaily is invoked as a daily cronjob to pull 2 day-old information
// from BigQuery to update the RTT database
func bqImportDaily(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	t = t.Add(time.Duration(-24 * 3 * time.Hour)) //Reduce time by 2 days
	rtt.BQImportDay(r, t)
}

// bqImportAllTime imports all available BigQuery RTT data
func bqImportAllTime(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	c.Warningf(`This job will import all data since 23rd June 2013.
		Be aware that there will be high resource usage, and that this
		action should be canceled as necessary by shutting the relevant
		instance down.`)

	start := time.Unix(1371945577, 0) // First RTT data entry in BigQuery is unix time 1371945577
	end := time.Now().Add(time.Duration(-24 * 2 * time.Hour))

	// Add day until exceeds 2 days ago
	day := time.Duration(24 * time.Hour)
	for time := start; time.Before(end); time = time.Add(day) {
		rtt.BQImportDay(r, time)
	}
}
