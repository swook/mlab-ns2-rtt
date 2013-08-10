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
	"appengine/urlfetch"
	"code.google.com/p/golog2bq/log2bq"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"
)

const URLBQDailyImport = "/rtt/cron.daily/import"

func init() {
	http.HandleFunc(URLBQDailyImport, bqImportDaily)
}

// bqImportDaily is invoked as a daily cronjob to pull 2 day-old information
// from BigQuery to update the RTT database
func bqImportDaily(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	t = t.Add(time.Duration(-24 * 2 * time.Hour)) //Reduce time by 2 days
	BQImportDay(r, t)
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
func bqInit(r *http.Request) (*bigquery.Service, error) {
	c := appengine.NewContext(r)

	// Get transport from log2bq's utility function GAETransport
	transport, err := log2bq.GAETransport(c, bigquery.BigqueryScope)
	if err != nil {
		return nil, err
	}

	// Set maximum urlfetch request deadline
	transport.Transport = &urlfetch.Transport{
		Context:  c,
		Deadline: time.Minute,
	}

	// Get http.Client from transport
	client, err := transport.Client()
	if err != nil {
		return nil, err
	}

	service, err := bigquery.New(client)
	return service, err
}

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
	c.Debugf("rtt: BQImportDay.qText (%s): %v\n", dateStr, qText)

	queryCall := bigquery.NewJobsService(service).Query("mlab-ns2", q)
	response, err := queryCall.Do()
	if err != nil {
		c.Errorf("rtt: BQImportDay.bigquery.JobsService.Query: %v", err)
		return
	}

	bqProcessQuery(c, response)
}

// bqProcessQuery processes the output of the BigQuery query performed in
// BQImport and parses the response into data structures.
func bqProcessQuery(c appengine.Context, r *bigquery.QueryResponse) {
	var prevClientIP, prevServerIP, clientIP, serverIP string
	var clientCG *ClientGroup
	var site *Site
	var rtt float64
	var rttdata *SiteRTT
	var lastUpdatedInt int64
	var err error
	CGsToSort := make(map[*ClientGroup]bool)

	for _, row := range r.Rows {
		serverIP = row.F[1].V.(string)
		if serverIP != prevServerIP {
			site = getSiteWithIP(net.ParseIP(serverIP))
			if site == nil {
				c.Errorf("rtt: bqProcessQuery.getSiteWithIP: %s is not associated with any site", serverIP)
				continue
			}
			prevServerIP = serverIP
		}

		clientIP = row.F[2].V.(string)
		if clientIP != prevClientIP {
			clientCG = getCGFromData(GetClientGroup(net.ParseIP(clientIP)))
			prevClientIP = clientIP
		}

		// Parse RTT from string entry
		// Use second last entry to exclude last hop
		rtt, err = strconv.ParseFloat(row.F[3].V.(string), 64)
		if err != nil {
			c.Errorf("rtt: bqProcessQuery.ParseFloat: %s", err)
			continue
		}

		// Compare with existing data and update if less
		rttdata = getSiteRTTFromCG(clientCG, site)
		if rttdata.RTT == 0.0 || rtt <= rttdata.RTT {
			rttdata.RTT = rtt

			// Update time on which RTT was logged
			lastUpdatedInt, err = strconv.ParseInt(row.F[0].V.(string), 10, 64)
			if err != nil {
				c.Errorf("rtt: bqProcessQuery.ParseInt: %s", err)
			}
			rttdata.LastUpdated = time.Unix(lastUpdatedInt, 0)

			// Mark ClientGroup as needing a sort on ClientGroup.SiteRTTs
			CGsToSort[clientCG] = true
		}
	}

	// Sort ClientGroups' SiteRTTs in ascending RTT order
	for cg, _ := range CGsToSort {
		sort.Sort(cg.SiteRTTs)
	}
}
