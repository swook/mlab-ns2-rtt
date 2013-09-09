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
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"code.google.com/p/mlab-ns2/gae/ns/data"
	"net"
	"sort"
	"strconv"
	"time"
)

const (
	dateFormat = "2006-01-02"
	timeFormat = "2006-01-02 15:04:05"
)

// bqRow is an intermediate data structure used to make data from BigQuery more
// accessible in the data processing and storing stage.
type bqRow struct {
	lastUpdated        time.Time
	serverIP, clientIP net.IP
	rtt                float64
}

// bqRows is a list of bqRow
type bqRows []*bqRow

// simplifyBQResponse takes BigQuery response rows and converts the string
// interface values into appropriate types. For example, rtt string is parsed
// into float64.
func simplifyBQResponse(rows []*bigquery.TableRow) bqRows {
	data := make(bqRows, 0, len(rows))

	var newRow *bqRow
	var lastUpdatedInt int64
	var err error

	for _, row := range rows {
		newRow = &bqRow{}
		newRow.serverIP = net.ParseIP(row.F[1].V.(string))
		if newRow.serverIP == nil {
			continue
		}
		newRow.clientIP = net.ParseIP(row.F[2].V.(string))
		if newRow.clientIP == nil {
			continue
		}
		newRow.rtt, err = strconv.ParseFloat(row.F[3].V.(string), 64)
		if err != nil {
			continue
		}
		lastUpdatedInt, err = strconv.ParseInt(row.F[0].V.(string), 10, 64)
		if err != nil {
			continue
		}
		newRow.lastUpdated = time.Unix(lastUpdatedInt, 0)
		data = append(data, newRow)
	}
	return data
}

// makeMapIPStrToSiteID creates a map of IP string to Site ID from SliverTools
// data from datastore.
func makeMapIPStrToSiteID(slivers []*data.SliverTool) map[string]string {
	ipToSliver := make(map[string]string)
	for _, s := range slivers {
		// TODO(seon.wook): Consider not branching within loop but using
		//                  delete after ipToSliver[""] = s.SiteID
		if s.SliverIPv4 != "" {
			ipToSliver[s.SliverIPv4] = s.SiteID
		}
		if s.SliverIPv6 != "" {
			ipToSliver[s.SliverIPv6] = s.SiteID
		}
	}
	return ipToSliver
}

// bqMergeIntoClientGroups merges new rows of data into an existing map of
// ClientGroup IP string to *ClientGroup. This involves the merging of new
// SiteRTTs with existing SiteRTTs, and the sorting of SiteRTTs to be in
// ascending RTT order.
func bqMergeIntoClientGroups(rows bqRows, sliverIPMap map[string]string, newCGs map[string]*ClientGroup) {
	var clientCGIP net.IP
	var clientCGIPStr string
	var clientCG *ClientGroup
	var siteID string
	var oldSR, newSR SiteRTT
	var oldSRIdx int
	var changed, ok bool

	// Slice of CGs which need to be sorted later on. This is because new
	// entries are inserted into an existing map and not all entries need
	// to be sorted.
	CGsToSort := make([]*ClientGroup, 0, len(rows))

	for _, row := range rows {
		// Get Site ID from serverIP
		siteID, ok = sliverIPMap[row.serverIP.String()]
		if !ok {
			continue
		}

		// Get ClientGroup.Prefix from clientIP
		clientCGIP = GetClientGroup(row.clientIP).IP
		clientCGIPStr = clientCGIP.String()
		// Create new ClientGroup if does not exist
		clientCG, ok = newCGs[clientCGIPStr]
		if !ok {
			clientCG = NewClientGroup(clientCGIP)
			newCGs[clientCGIPStr] = clientCG
		}

		// Find SiteRTT entry
		ok = false // Shows if entry exists
		for i, sitertt := range clientCG.SiteRTTs {
			if sitertt.SiteID == siteID {
				// Found entry
				oldSRIdx = i
				oldSR = sitertt
				ok = true
			}
		}

		// Create new entry
		newSR = SiteRTT{siteID, row.rtt, row.lastUpdated}
		if !ok {
			// No existing entry, add new entry
			clientCG.SiteRTTs = append(clientCG.SiteRTTs, newSR)
			changed = true
		} else {
			// Entry exists, merge with old entry
			// NOTE: Can ignore error as error only occurs when oldSR.SiteID
			//       != newSR.SiteID.
			changed, _ = MergeSiteRTTs(&oldSR, &newSR)
			if changed {
				clientCG.SiteRTTs[oldSRIdx] = oldSR
			}
		}
		if changed { // If existing SiteRTTs changed or updated
			CGsToSort = append(CGsToSort, clientCG)
		}
	}

	// Sort ClientGroups' SiteRTTs in ascending RTT order
	// TODO(seon.wook): Consider better sorting algo in cases such as almost
	//                  sorted lists.
	for _, cg := range CGsToSort {
		sort.Sort(cg.SiteRTTs)
	}
}
