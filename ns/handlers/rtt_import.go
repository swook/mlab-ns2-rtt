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
	"errors"
	"net/http"
	"time"
)

var (
	ErrNoDateSpecified = errors.New("rtt: No date specified in request.")
	ErrInvalidDate     = errors.New("rtt: Invalid date specified in request.")
)

func init() {
	http.HandleFunc(rtt.URLImportDaily, rttImportDaily)
	http.HandleFunc(rtt.URLImportAll, rttImportAllTime)
}

// rttImportDay imports bigquery data for a specified day.
func rttImportDay(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	dateStr := r.FormValue(rtt.FormKeyImportDate)
	if dateStr == "" {
		http.Error(w, ErrNoDateSpecified.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.bqImportDay:http.Request.FormValue: %v", ErrNoDateSpecified)
		return
	}
	t, err := time.Parse(rtt.DateFormat, dateStr)
	if err != nil {
		http.Error(w, ErrInvalidDate.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.bqImportDay:time.Parse: %v", ErrInvalidDate)
		return
	}
	addTaskRTTImportDay(w, r, t)
}

// rttImportDaily is invoked as a daily cronjob to pull 2 day-old information
// from BigQuery to update the RTT database
func rttImportDaily(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	t = t.Add(time.Duration(-24 * 2 * time.Hour)) //Reduce time by 2 days
	addTaskRTTImportDay(w, r, t)
}

// rttImportAllTime imports all available BigQuery RTT data
func rttImportAllTime(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	c.Warningf(`This job will import all data since 23rd June 2013.
		Be aware that there will be high resource usage, and that this
		action should be canceled as necessary by shutting the relevant
		instance down.`)

	start := rtt.EarliestTimewithRTTData // First RTT data entry in BigQuery is unix time 1371945577
	end := time.Now().Add(time.Duration(-24 * 2 * time.Hour))

	// Add day until exceeds 2 days ago
	day := time.Duration(24 * time.Hour)
	for time := start; time.Before(end); time = time.Add(day) {
		rtt.BQImportDay(w, r, time)
	}
}
