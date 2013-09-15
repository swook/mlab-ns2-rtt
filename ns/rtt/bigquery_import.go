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
	"appengine/urlfetch"
	"code.google.com/p/golog2bq/log2bq"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"code.google.com/p/mlab-ns2/gae/ns/data"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const (
	MaxBQResponseRows         = 50000 //Response size must be less than 32MB. 100k rows occasionally caused problems.
	BigQueryBillableProjectID = "mlab-ns2"
)

var ErrNoBigQueryData = errors.New("No BigQuery rows received in response from query.")

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
//
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
		paris_traceroute_hop.dest_ip;`

// bqInit authenticates a transport using OAuth and returns a *bigquery.Service
// with which to make queries to bigquery.
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
		Deadline: 10 * time.Minute,
	}

	client, err := transport.Client()
	if err != nil {
		return nil, err
	}

	return bigquery.New(client)
}

// BQImportDay queries BigQuery for RTT data from a specific day and stores new
// data into datastore
func BQImportDay(w http.ResponseWriter, r *http.Request, t time.Time) {
	c := appengine.NewContext(r)
	service, err := bqInit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("rtt: BQImportDay.bqInit: %s", err)
		return
	}

	// Format strings to insert into bqQueryFormat
	tableName := fmt.Sprintf("measurement-lab:m_lab.%.4d_%.2d", t.Year(), t.Month())
	dateStr := t.Format(DateFormat)
	startTime, endTime := getDayStartEnd(t)

	// Construct query
	qText := fmt.Sprintf(bqQueryFormat, tableName, startTime.Unix(), endTime.Unix())
	q := &bigquery.QueryRequest{
		Query:         qText,
		MaxResults:    MaxBQResponseRows,
		TimeoutMs:     600000,
		UseQueryCache: true,
	}
	c.Debugf("rtt.BQImportDay:bigquery.QueryRequest (%s): %s", dateStr, qText)

	// Make first query to BigQuery
	jobsService := bigquery.NewJobsService(service)
	queryCall := jobsService.Query(BigQueryBillableProjectID, q)
	response, err := queryCall.Do()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("rtt.BQImportDay:bigquery.JobsService.Query: %s", err)
		return
	}
	c.Infof("rtt: Received %d rows in query response (Total: %d rows).", len(response.Rows), response.TotalRows)

	newCGs := make(map[string]*ClientGroup)
	sliverTools, err := data.GetSliverTools(c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		c.Errorf("rtt.BQImportDay:data.GetSliverTools: %s", err)
	}
	sliverIPMap := makeMapIPStrToSiteID(sliverTools)
	sliverTools = nil // mark for GC
	bqProcessQuery(response.Rows, sliverIPMap, newCGs)

	// Cache details from response to use in subsequent requests if any.
	projID := response.JobReference.ProjectId
	jobID := response.JobReference.JobId
	pageToken := response.PageToken
	n := len(response.Rows)
	totalN := int(response.TotalRows)
	response = nil // mark for GC

	// Request for more results if not all results returned.
	// TODO(seon.wook): Good place to look for optimizations using channels.
	if n < totalN {
		// Make further requests
		getQueryResultsCall := jobsService.GetQueryResults(projID, jobID)
		var respMore *bigquery.GetQueryResultsResponse

		for n < totalN { // Make requests until total number of rows queried.
			getQueryResultsCall.MaxResults(MaxBQResponseRows)
			getQueryResultsCall.PageToken(pageToken)

			respMore, err = getQueryResultsCall.Do()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				c.Errorf("rtt.BQImportDay:bigquery.JobsGetQueryResponseCall: %s", err)
				return
			}
			pageToken = respMore.PageToken // Update pageToken to get next page.

			n += len(respMore.Rows)
			c.Infof("rtt: Received %d additional rows. (Total: %d rows)", len(respMore.Rows), n)

			bqProcessQuery(respMore.Rows, sliverIPMap, newCGs)
			respMore = nil // mark for GC
		}
	}

	c.Infof("rtt: Reduced %d rows to %d rows. Merging into datastore.", totalN, len(newCGs))

	if totalN == 0 {
		http.Error(w, ErrNoBigQueryData.Error(), http.StatusInternalServerError)
		c.Errorf("rtt.BQImportDay: %s", ErrNoBigQueryData)
		return
	}

	bqMergeWithDatastore(c, dateStr, newCGs)
}

// bqProcessQuery processes the output of the BigQuery query performed in
// BQImport and parses the response into data structures.
func bqProcessQuery(resp []*bigquery.TableRow, sliverIPMap map[string]string, newCGs map[string]*ClientGroup) {
	rows := simplifyBQResponse(resp)
	bqMergeIntoClientGroups(rows, sliverIPMap, newCGs)
}

// bqMergeWithDatastore takes a list of ClientGroup generated by bqProcessQuery
// and merges the new data with existing data in datastore.
func bqMergeWithDatastore(c appengine.Context, dateStr string, newCGs map[string]*ClientGroup) {
	chunks := divideIntoDSReadChunks(c, newCGs)

	var oldCGs []ClientGroup
	var newCG *ClientGroup
	var err error
	var merr []error
	var ok, changed bool

	putReq := &putQueueRequest{}

	// Process chunk by chunk
	for _, chunk := range chunks {
		oldCGs = make([]ClientGroup, chunk.len())
		err = datastore.GetMulti(c, chunk.keys, oldCGs) // Get existing ClientGroup data

		merr, ok = err.(appengine.MultiError)
		if !ok {
			// If error not multi, create list of nil errors. This is done to
			// allow for one loop over all queried ClientGroups below.
			merr = make([]error, chunk.len())
		}

		// Range over all entries in chunk
		for i, e := range merr {
			newCG, changed = bqMergeCGWithDS(c, &oldCGs[i], chunk.cgs[i], e)
			if changed {
				putReq.add(c, dateStr, chunk.keys[i], newCG)
			}
		}
	}

	// Process remaining Put operations.
	putReq.process(c, dateStr)

	c.Infof("rtt: Completed merging in %d rows from BigQuery.", len(newCGs))
}

// bqMergeCGWithDS deals with a response from datastore.Get for the entity
// ClientGroup
func bqMergeCGWithDS(c appengine.Context, oldCG, newCG *ClientGroup, err error) (*ClientGroup, bool) {
	switch err {
	// No stored entity
	case datastore.ErrNoSuchEntity:
		return newCG, true
	// No error
	case nil:
		// If old data is nil for some reason
		if oldCG.SiteRTTs == nil {
			return newCG, true
		}

		// Merge new CG with old CG
		changed, err := MergeClientGroups(oldCG, newCG)
		if err != nil {
			c.Errorf("rtt.bqMergeCGWithDS: %s", err)
		}
		return oldCG, changed
	// Unknown error
	default:
		c.Errorf("rtt.bqMergeCGWithDS: %s", err)
	}
	return oldCG, false
}
