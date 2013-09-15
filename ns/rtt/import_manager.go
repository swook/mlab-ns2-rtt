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

package rtt

import (
	"appengine"
	"appengine/datastore"
	"time"
)

var EarliestTimewithRTTData = time.Unix(1371945577, 0)

// GetLastSuccesfulImportDate returns the last recorded time of a successful
// bigquery import.
func GetLastSuccesfulImportDate(c appengine.Context) (time.Time, error) {
	key := datastore.NewKey(c, "time.Time", DSKeyLastSuccImport, 0, nil)
	var t time.Time
	err := datastore.Get(c, key, &t)
	if err == datastore.ErrNoSuchEntity {
		return EarliestTimewithRTTData, nil
	} else if err != nil {
		return t, err
	}
	return t, nil
}

// SetLastSuccesfulImportDate sets a time as the last recorded time of a
// successful bigquery import.
func SetLastSuccessfulImportDate(c appengine.Context, t time.Time) error {
	key := datastore.NewKey(c, "time.Time", DSKeyLastSuccImport, 0, nil)
	if _, err := datastore.Put(c, key, t); err != nil {
		return err
	}
	return nil
}

// GetNextImportDay returns the next day for which to perform a bigquery import,
// calculated by adding a day to the date of the latest successful import.
func GetNextImportDay(c appengine.Context) time.Time {
	t, err := GetLastSuccesfulImportDate(c)
	if err != nil {
		t = EarliestTimewithRTTData
	}
	return t.Add(Day)
}
