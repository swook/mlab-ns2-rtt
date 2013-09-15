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
	"code.google.com/p/mlab-ns2/gae/ns/data"
	"time"
)

const DSKeyStats = "rtt.Stats"

var EarliestTimewithRTTData = time.Unix(1371945577, 0)

type Stats struct {
	LastSuccessfulImportDate time.Time
}

// GetLastSuccesfulImportDate returns the last recorded time of a successful
// bigquery import.
func GetLastSuccesfulImportDate(c appengine.Context) (time.Time, error) {
	key := datastore.NewKey(c, "Stats", DSKeyStats, 0, DatastoreParentKey(c))
	var s Stats
	err := data.GetData(c, DSKeyStats, key, &s)
	if err == datastore.ErrNoSuchEntity {
		return EarliestTimewithRTTData, nil
	} else if err != nil {
		return s.LastSuccessfulImportDate, err
	}
	return s.LastSuccessfulImportDate, nil
}

// SetLastSuccesfulImportDate sets a time as the last recorded time of a
// successful bigquery import.
func SetLastSuccessfulImportDate(c appengine.Context, t time.Time) error {
	key := datastore.NewKey(c, "Stats", DSKeyStats, 0, DatastoreParentKey(c))
	var s Stats
	if err := data.GetData(c, DSKeyStats, key, &s); err != datastore.ErrNoSuchEntity && err != nil {
		return err
	}
	s.LastSuccessfulImportDate = t
	if err := data.SetData(c, DSKeyStats, key, &s); err != nil {
		return err
	}
	return nil
}

// UpdateLastSuccesfulImportDate sets a time as the last recorded time of a
// successful bigquery import if the provided time is newer than the recorded
// time.
func UpdateLastSuccessfulImportDate(c appengine.Context, t time.Time) error {
	last, err := GetLastSuccesfulImportDate(c)
	if err != nil {
		return err
	}

	// Don't update if time provided is not newer.
	if !last.Before(t) {
		return nil
	}

	c.Infof("rtt: Updated the last successful date imported to: %s", t)
	return SetLastSuccessfulImportDate(c, t)
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
