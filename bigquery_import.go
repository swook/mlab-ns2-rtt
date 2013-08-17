// +build appengine

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
	"appengine"
	"appengine/datastore"
	// "appengine/urlfetch"
	// "code.google.com/p/golog2bq/log2bq"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"
)

const (
	URLBQImportDaily   = "/rtt/import/daily"
	URLBQImportAll     = "/rtt/import/all"
	MaxDSWritePerQuery = 500
)

func init() {
	http.HandleFunc(URLBQImportDaily, bqImportDaily)
	http.HandleFunc(URLBQImportAll, bqImportAllTime)
}

// bqImportDaily is invoked as a daily cronjob to pull 2 day-old information
// from BigQuery to update the RTT database
func bqImportDaily(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	t = t.Add(time.Duration(-24 * 2 * time.Hour)) //Reduce time by 2 days
	BQImportDay(r, t)
}

// bqImportAllTime imports all available BigQuery RTT data
func bqImportAllTime(w http.ResponseWriter, r *http.Request) {
	start := time.Unix(1371945577, 0) // First RTT data entry in BigQuery is unix time 1371945577
	end := time.Now().Add(time.Duration(-24 * 2 * time.Hour))

	// Add day until exceeds 2 days ago
	day := time.Duration(24 * time.Hour)
	for time := start; time.Before(end); time = time.Add(day) {
		BQImportDay(r, time)
	}
}

// bqQueryFormat is the query used to pull RTT data from the M-Lab BigQuery
// dataset.
// NOTE: It must be formatted with Table Name, Year, Month, Day, Year, Month,
// Day to specify which day the query is being performed for.
//
// The following columns are selected:
// - Logged Time (log_time)
// - M-Lab Server IP (connection_spec.server_ip)
// - Destination IP for traceroute hop, towards client (paris_traceroute_hop.dest_ip)
// - Average of RTT in same traceroute and hop
//
// The Query is performed for entries logged on specified days and for cases
// where the field paris_traceroute_hop.rtt is not null. (RTT data exists)
// The query also excludes RTT to the client due to the variability of the last
// hop.
//
// The result is grouped by time and the IPs such that multiple traceroute rtt
// entries can be averaged.

// The result is ordered by server and client IPs to allow for more efficient
// traversal of response entries.
const bqQueryFormat = `SELECT
		log_time,
		connection_spec.server_ip,
		paris_traceroute_hop.dest_ip,
		AVG(paris_traceroute_hop.rtt) AS rtt
	FROM [%s]
	WHERE
		project = 3 AND
		log_time > %d AND
		log_time < %d AND
		log_time IS NOT NULL AND
		connection_spec.server_ip IS NOT NULL AND
		paris_traceroute_hop.dest_ip IS NOT NULL AND
		paris_traceroute_hop.rtt IS NOT NULL AND
		connection_spec.client_ip != paris_traceroute_hop.dest_ip
	GROUP EACH BY
		log_time,
		connection_spec.server_ip,
		paris_traceroute_hop.dest_ip
	ORDER BY
		paris_traceroute_hop.dest_ip,
		connection_spec.server_ip;`

// bqInit logs in to bigquery using OAuth and returns a *bigquery.Service with
// which to make queries to bigquery.
// func bqInit(r *http.Request) (*bigquery.Service, error) {
// 	c := appengine.NewContext(r)

// 	// Get transport from log2bq's utility function GAETransport
// 	transport, err := log2bq.GAETransport(c, bigquery.BigqueryScope)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Set maximum urlfetch request deadline
// 	transport.Transport = &urlfetch.Transport{
// 		Context:  c,
// 		Deadline: 10 * time.Minute,
// 	}

// 	client, err := transport.Client()
// 	if err != nil {
// 		return nil, err
// 	}

// 	service, err := bigquery.New(client)
// 	return service, err
// }

const (
	dateFormat = "2006-01-02"
	timeFormat = "2006-01-02 15:04:05"
)

// BQImportDay queries BigQuery for RTT data from a specific day and stores new
// data into datastore
func BQImportDay(r *http.Request, t time.Time) {
	c := appengine.NewContext(r)
	service, err := bqInit(r)
	if err != nil {
		c.Errorf("rtt: BQImportDay.bqInit: %s", err)
		return
	}

	// Format strings to insert into bqQueryFormat
	tableName := fmt.Sprintf("measurement-lab:m_lab.%.4d_%.2d", t.Year(), t.Month())
	dateStr := t.Format(dateFormat)
	startTime, _ := time.Parse(timeFormat, dateStr+" 00:00:00")
	endTime, _ := time.Parse(timeFormat, dateStr+" 23:59:59")

	// Construct query
	qText := fmt.Sprintf(bqQueryFormat, tableName, startTime.Unix(), endTime.Unix())
	q := &bigquery.QueryRequest{
		Query:     qText,
		TimeoutMs: 60000,
	}
	c.Debugf("rtt: BQImportDay.qText (%s): %s", dateStr, qText)

	queryCall := bigquery.NewJobsService(service).Query("mlab-ns2", q)
	response, err := queryCall.Do()
	if err != nil {
		c.Errorf("rtt: BQImportDay.bigquery.JobsService.Query: %s", err)
		return
	}
	c.Debugf("rtt: Received %d rows in query response.", len(response.Rows))

	newCGs := bqProcessQuery(c, response)
	c.Debugf("rtt: Reduced query response to %d rows. Merging into datastore.", len(newCGs))

	bqMergeWithDatastore(c, newCGs)
}

// bqProcessQuery processes the output of the BigQuery query performed in
// BQImport and parses the response into data structures.
func bqProcessQuery(c appengine.Context, r *bigquery.QueryResponse) map[string]*ClientGroup {
	var prevClientIP, prevServerIP, clientIP, serverIP string
	var clientCGIP net.IP
	var clientCGIPStr string
	var clientCG *ClientGroup
	var site *Site
	var rtt float64
	var rttData SiteRTT
	var rttDataIdx int
	var lastUpdatedInt int64
	var ok bool
	var err error

	CGs := make(map[string]*ClientGroup)

	for _, row := range r.Rows {
		serverIP = row.F[1].V.(string)
		if serverIP != prevServerIP {
			site, ok = SliversDB[serverIP]
			if !ok {
				c.Errorf("rtt: bqProcessQuery.getSiteWithIP: %s is not associated with any site", serverIP)
				continue
			}
			prevServerIP = serverIP
		}

		clientIP = row.F[2].V.(string)
		if clientIP != prevClientIP {
			clientCGIP = GetClientGroup(net.ParseIP(clientIP)).IP
			clientCGIPStr = clientCGIP.String()
			clientCG, ok = CGs[clientCGIPStr]
			if !ok {
				clientCG = NewClientGroup(clientCGIP)
				CGs[clientCGIPStr] = clientCG
			}
			prevClientIP = clientIP
		}

		// Parse RTT from string entry
		// Use second last entry to exclude last hop
		rtt, err = strconv.ParseFloat(row.F[3].V.(string), 64)
		if err != nil {
			c.Errorf("rtt: bqProcessQuery.ParseFloat: %s", err)
			continue
		}

		// Insert into SiteRTTs list
		ok = false
		for i, sitertt := range clientCG.SiteRTTs {
			if sitertt.SiteID == site.ID {
				rttDataIdx = i
				rttData = sitertt
				ok = true
			}
		}
		if !ok {
			rttDataIdx = len(clientCG.SiteRTTs)
			rttData = SiteRTT{SiteID: site.ID}
			clientCG.SiteRTTs = append(clientCG.SiteRTTs, rttData)
		}

		// If rtt data has not been recorded or if rtt is less than existing data's rtt.
		if !ok || rtt <= rttData.RTT {
			rttData.RTT = rtt

			// Update time on which RTT was logged
			lastUpdatedInt, err = strconv.ParseInt(row.F[0].V.(string), 10, 64)
			if err != nil {
				c.Errorf("rtt: bqProcessQuery.ParseInt: %s", err)
			}
			rttData.LastUpdated = time.Unix(lastUpdatedInt, 0)
			clientCG.SiteRTTs[rttDataIdx] = rttData
		}
	}

	// Sort ClientGroups' SiteRTTs in ascending RTT order
	for _, cg := range CGs {
		sort.Sort(cg.SiteRTTs)
	}

	return CGs
}

// dsWriteChunk is a structure with which new ClientGroup lists can be split
// into lengths <= MaxDSWritePerQuery such that datastore.PutMulti works.
type dsWriteChunk struct {
	Keys []*datastore.Key
	CGs  []*ClientGroup
}

// bqMergeWithDatastore takes a list of ClientGroup generated by bqProcessQuery
// and merges the new data with existing data in datastore
func bqMergeWithDatastore(c appengine.Context, newCGs map[string]*ClientGroup) {
	rttKey := datastore.NewKey(c, "string", "rtt", 0, nil)

	// Divide GetMulti and PutMulti operations into MaxDSWritePerQuery sized
	// operations to adhere with GAE limits.
	chunks := make([]*dsWriteChunk, 0)
	var thisChunk *dsWriteChunk
	newChunk := func() {
		thisChunk = &dsWriteChunk{
			Keys: make([]*datastore.Key, 0, MaxDSWritePerQuery),
			CGs:  make([]*ClientGroup, 0, MaxDSWritePerQuery),
		}
		chunks = append(chunks, thisChunk)
	}
	newChunk()
	for cgStr, cg := range newCGs {
		thisChunk.Keys = append(thisChunk.Keys, datastore.NewKey(c, "ClientGroup", cgStr, 0, rttKey))
		thisChunk.CGs = append(thisChunk.CGs, cg)
		if len(thisChunk.CGs) == MaxDSWritePerQuery {
			newChunk()
		}
	}

	var oldCGs []ClientGroup
	var merr appengine.MultiError
	var err error

	for _, chunk := range chunks {
		oldCGs = make([]ClientGroup, len(chunk.CGs))
		err = datastore.GetMulti(c, chunk.Keys, oldCGs)

		switch err.(type) {
		case appengine.MultiError: // Multiple errors, deal with individually
			merr = err.(appengine.MultiError)
			for i, e := range merr { // Range over errors
				switch e {
				case datastore.ErrNoSuchEntity: // New entry
					oldCGs[i] = *chunk.CGs[i]
				case nil: // Entry exists, merge new data with old data
					if err := MergeClientGroups(&oldCGs[i], chunk.CGs[i]); err != nil {
						c.Errorf("rtt: bqMergeWithDatastore.MergeClientGroups: %s", err)
					}
				default: // Other unknown error
					c.Errorf("rtt: bqMergeWithDatastore.datastore.GetMulti: %s", err)
				}
			}
		case nil: // No errors, data exists so merge with old data
			for i, _ := range oldCGs {
				if err := MergeClientGroups(&oldCGs[i], chunk.CGs[i]); err != nil {
					c.Errorf("rtt: bqMergeWithDatastore.MergeClientGroups: %s", err)
				}
			}
		default: // Other unknown errors from GetMulti
			c.Errorf("rtt: bqMergeWithDatastore.datastore.GetMulti: %s", err)
		}

		// Put updated data set to datastore
		_, err = datastore.PutMulti(c, chunk.Keys, oldCGs)
		if err != nil {
			c.Errorf("rtt: bqMergeWithDatastore.datastore.PutMulti: %s", err)
		}
	}
}
