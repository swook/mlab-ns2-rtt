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
	"fmt"
	"net/http"
	"time"
)

const (
	URLRTTImportDay   = "/admin/rtt/import/day"
	URLRTTImportDaily = "/admin/rtt/import/daily"
	URLRTTImportAll   = "/admin/rtt/import/all"

	URLRTTSetLastSuccImportDate = "/admin/rtt/import/setLastSuccessfulDate"
)

func init() {
	http.HandleFunc(URLRTTImportDay, rttImportDay)
	http.HandleFunc(URLRTTImportDaily, rttImportDaily)
	http.HandleFunc(URLRTTSetLastSuccImportDate, rttSetLastSuccImportDate)
}

// rttImportDay imports bigquery data for a specified day.
func rttImportDay(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	dateStr := r.FormValue(rtt.FormKeyImportDate)
	t, err := time.Parse(rtt.DateFormat, dateStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.rttImportDay:time.Parse: %v", err)
		return
	}
	addTaskRTTImportDay(w, r, t)
}

// rttImportDaily is invoked as a daily cronjob to pull 2 day-old information
// from BigQuery to update the RTT database
func rttImportDaily(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	now := time.Now()
	latest := now.Add(time.Duration(-24 * 2 * time.Hour)) //Reduce time by 2 days
	next := rtt.GetNextImportDay(c)

	if next.Before(latest) {
		addTaskRTTImportDay(w, r, next)
	} else {
		c.Infof("handlers.rttImportDaily: Nothing to import.")
	}
}

// rttSetLastSuccImportDate
func rttSetLastSuccImportDate(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	dateStr := r.FormValue(rtt.FormKeyImportDate)
	t, err := time.Parse(rtt.DateFormat, dateStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.rttSetLastSuccImportDate:time.Parse: %v", err)
		return
	}

	if err := rtt.SetLastSuccessfulImportDate(c, t); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.rttSetLastSuccImportDate:rtt.SetLastSuccessfulImportDate: %v", err)
		return
	}

	fmt.Fprintf(w, "Last successful bigquery import date now set to: %s", t)
}
