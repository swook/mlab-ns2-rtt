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
	"appengine/memcache"
	"appengine/taskqueue"
	"code.google.com/p/mlab-ns2/gae/ns/rtt"
	"net"
	"net/http"
	"net/url"
	"time"
)

func init() {
	http.HandleFunc(rtt.URLTaskImportDay, processTaskRTTImportDay)
	http.HandleFunc(rtt.URLTaskImportPut, processTaskRTTCGPut)
}

// addTaskRTTImportDay adds a BigQuery import task into taskqueue for a
// specified date.
func addTaskRTTImportDay(w http.ResponseWriter, r *http.Request, t time.Time) {
	c := appengine.NewContext(r)

	date := t.Format(rtt.DateFormat)

	c.Infof("handlers: Submitting BQ import task for %s", date)

	values := make(url.Values)
	values.Add(rtt.FormKeyImportDate, date)
	task := taskqueue.NewPOSTTask(rtt.URLTaskImportDay, values)
	_, err := taskqueue.Add(c, task, rtt.TaskQueueNameImport)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.addTaskRTTImportDay:taskqueue.Add: %s", err)
		return
	}
}

// processTaskRTTImportDay processes a taskqueue task for an import of BigQuery
// data for a specified date.
func processTaskRTTImportDay(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	dateStr := r.FormValue(rtt.FormKeyImportDate)
	t, err := time.Parse(rtt.DateFormat, dateStr)
	if err != nil {
		// Don't return HTTP error since incorrect date cannot be fixed.
		c.Errorf("handlers.processTaskRTTImportDay:time.Parse: %s", err)
		return
	}

	rtt.BQImportDay(w, r, t)
}

// processTaskRTTCGPut processes a taskqueue task for the putting of new
// ClientGroups into datastore.
func processTaskRTTCGPut(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	// Get memcache key to use from POST parameters
	dataKey := r.FormValue(rtt.FormKeyPutKey)
	var data []rtt.ClientGroup
	_, err := memcache.Gob.Get(c, dataKey, &data)
	if err != nil {
		// Don't return HTTP error since nothing can be done if data
		// is missing or corrupt. Just log to GAE to see how often this
		// happens.
		c.Errorf("handlers.processTaskRTTCGPut:memcache.Get: %s", err)
		return
	}

	// Create lists of keys to use in datastore.PutMulti
	parentKey := rtt.DatastoreParentKey(c)
	keys := make([]*datastore.Key, 0, len(data))
	var key *datastore.Key
	for _, cg := range data {
		key = datastore.NewKey(c, "ClientGroup", net.IP(cg.Prefix).String(), 0, parentKey)
		keys = append(keys, key)
	}

	// Put data into datastore
	_, err = datastore.PutMulti(c, keys, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.processTaskRTTCGPut:datastore.PutMulti: %s", err)
		return
	}

	// Remove cached CGs
	if err := memcache.Delete(c, dataKey); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.processTaskRTTCGPut:memcache.Delte: %s", err)
		return
	}

	dateStr := r.FormValue(rtt.FormKeyImportDate)
	c.Infof("handlers: %d ClientGroups were successfully put into datastore. (%s)", len(data), dateStr)

	// Get which date this import is for
	t, err := time.Parse(rtt.DateFormat, dateStr)
	if err != nil {
		// Don't return HTTP error since incorrect date cannot be fixed.
		c.Errorf("handlers.processTaskRTTCGPut:time.Parse: %s", err)
		return
	}
	rtt.UpdateLastSuccessfulImportDate(c, t)
}
