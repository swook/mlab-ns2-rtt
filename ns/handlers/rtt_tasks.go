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
	"net/http"
	"net/url"
	"time"
)

func init() {
	http.HandleFunc(rtt.URLTaskImportDay, processTaskRTTImportDay)
	http.HandleFunc(rtt.URLTaskImportPut, processTaskRTTCGPut)
}

func addTaskRTTImportDay(w http.ResponseWriter, r *http.Request, t time.Time) {
	c := appengine.NewContext(r)

	date := t.Format(rtt.DateFormat)

	c.Infof("handlers.addTaskRTTImportDay: Submitting BQ import task for %s", date)

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

func processTaskRTTImportDay(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	dateStr := r.FormValue(rtt.FormKeyImportDate)
	t, err := time.Parse(rtt.DateFormat, dateStr)
	if err != nil {
		http.Error(w, ErrInvalidDate.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.processTaskRTTImportDay:time.Parse: %s", ErrInvalidDate)
		return
	}

	rtt.BQImportDay(w, r, t)
}

func processTaskRTTCGPut(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	// Get memcache key to use from POST parameters
	dataKey := r.FormValue(FormKeyRTTPutKey)
	var data map[string]rtt.ClientGroup
	_, err := memcache.Gob.Get(c, dataKey, &data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.processTaskRTTDSPutMulti:memcache.Get: %s", err)
		return
	}

	// Create lists of keys and ClientGroups to use in datastore.PutMulti
	n := len(data)
	parentKey := rtt.DatastoreParentKey(c)
	keys := make([]*datastore.Key, 0, n)
	cgs := make([]rtt.ClientGroup, 0, n)
	for k, cg := range data {
		keys = append(keys, datastore.NewKey(c, "ClientGroup", k, 0, parentKey))
		cgs = append(cgs, cg)
	}
	data = nil // Mark map[string]ClientGroup for GC

	// Perform datastore.PutMulti
	_, err = datastore.PutMulti(c, keys, cgs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("handlers.processTaskRTTDSPutMulti:datastore.PutMulti: %s", err)
		return
	}
}
