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
	// "appengine/urlfetch"
	// "code.google.com/p/golog2bq/log2bq"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"fmt"
	"net"
	"net/http"
	"sort"
	"time"
)

const (
	URLBQImportDaily          = "/rtt/import/daily"
	URLBQImportAll            = "/rtt/import/all"
	MaxDSReadPerQuery         = 1000
	MaxDSWritePerQuery        = 500
	MaxBQResponseRows         = 50000 //Response size must be less than 32MB. 100k rows occasionally caused problems.
	BigQueryBillableProjectID = "mlab-ns2"
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
		paris_traceroute_hop.dest_ip;`

// bqInit authenticates a transport using OAuth and returns a *bigquery.Service
// with which to make queries to bigquery.
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
		Query:         qText,
		MaxResults:    MaxBQResponseRows,
		TimeoutMs:     600000,
		UseQueryCache: true,
	}
	c.Debugf("rtt: BQImportDay.qText (%s): %s", dateStr, qText)

	// Make first query to BigQuery
	jobsService := bigquery.NewJobsService(service)
	queryCall := jobsService.Query(BigQueryBillableProjectID, q)
	response, err := queryCall.Do()
	if err != nil {
		c.Errorf("rtt: BQImportDay.bigquery.JobsService.Query: %s", err)
		return
	}
	c.Infof("rtt: Received %d rows in query response (Total: %d rows).", len(response.Rows), response.TotalRows)

	data := make(map[string]*ClientGroup)
	bqProcessQuery(c, response.Rows, data)

	// Cache details from response to use in subsequent requests if any.
	projID := response.JobReference.ProjectId
	jobID := response.JobReference.JobId
	pageToken := response.PageToken
	n := len(response.Rows)
	totalN := int(response.TotalRows)
	response = nil

	// Request for more results if not all results returned.
	if n < totalN {
		// Make further requests
		getQueryResultsCall := jobsService.GetQueryResults(projID, jobID)
		var respMore *bigquery.GetQueryResultsResponse

		for n < totalN { // Make requests until total number of rows queried.
			getQueryResultsCall.MaxResults(MaxBQResponseRows)
			getQueryResultsCall.PageToken(pageToken)

			respMore, err = getQueryResultsCall.Do()
			if err != nil {
				c.Errorf("rtt: BQImportDay.bigquery.JobsGetQueryResponseCall: %s", err)
				return
			}
			pageToken = respMore.PageToken // Update pageToken to get next page.

			n += len(respMore.Rows)
			c.Infof("rtt: Received %d additional rows. (Total: %d rows)", len(respMore.Rows), n)

			bqProcessQuery(c, respMore.Rows, data)
			respMore = nil
		}
	}

	c.Infof("rtt: Reduced %d rows to %d rows. Merging into datastore.", totalN, len(data))

	bqMergeWithDatastore(c, data)
}

// bqProcessQuery processes the output of the BigQuery query performed in
// BQImport and parses the response into data structures.
// TODO(gavaletz): Evaluate whether this func could be split into smaller funcs.
func bqProcessQuery(c appengine.Context, response []*bigquery.TableRow, data map[string]*ClientGroup) {
	// Change interface and string values in table rows to what they should be:
	// net.IP, time.Time, float64 (RTT).
	rows := simplifyBQResponse(response)

	var clientCGIP net.IP
	var clientCGIPStr string
	var clientCG *ClientGroup
	var site *Site
	var rttData SiteRTT
	var rttDataIdx int
	var ok bool

	// Slice of CGs which need to be sorted later on. This is because new
	// entries are inserted into an existing map and not all entries need
	// to be sorted.
	CGsToSort := make([]*ClientGroup, 0, len(rows))

	for _, row := range rows {
		// Get Site ID from serverIP
		site, ok = SliversDB[row.serverIP.String()]
		if !ok {
			c.Errorf("rtt: bqProcessQuery.getSiteWithIP: %s is not associated with any site", row.serverIP)
			continue
		}

		// Get ClientGroup.Prefix from clientIP
		clientCGIP = GetClientGroup(row.clientIP).IP
		clientCGIPStr = clientCGIP.String()
		// Create new ClientGroup if does not exist
		clientCG, ok = data[clientCGIPStr]
		if !ok {
			clientCG = NewClientGroup(clientCGIP)
			data[clientCGIPStr] = clientCG
		}

		// Find SiteRTT entry or insert new one
		ok = false
		for i, sitertt := range clientCG.SiteRTTs {
			if sitertt.SiteID == site.ID {
				// Found entry
				rttDataIdx = i
				rttData = sitertt
				ok = true
			}
		}
		if !ok {
			// No existing entry, create new entry
			rttDataIdx = len(clientCG.SiteRTTs)
			rttData = SiteRTT{SiteID: site.ID}
			clientCG.SiteRTTs = append(clientCG.SiteRTTs, rttData)
		}

		// If rtt data has not been recorded or if new rtt is less than existing
		// data's rtt, use new rtt data
		if !ok || row.rtt <= rttData.RTT {
			rttData.RTT = row.rtt
			rttData.LastUpdated = row.lastUpdated
			clientCG.SiteRTTs[rttDataIdx] = rttData
			CGsToSort = append(CGsToSort, clientCG)
		}
	}

	// Sort ClientGroups' SiteRTTs in ascending RTT order
	for _, cg := range CGsToSort {
		sort.Sort(cg.SiteRTTs)
	}
}

// dsWriteChunk is a structure with which new ClientGroup lists can be split
// into lengths <= MaxDSWritePerQuery such that datastore.PutMulti works.
type dsReadChunk struct {
	Keys []*datastore.Key
	CGs  []*ClientGroup
}

// bqMergeWithDatastore takes a list of ClientGroup generated by bqProcessQuery
// and merges the new data with existing data in datastore
// TODO(gavaletz): Evaluate whether this func could be split into smaller funcs,
// also consider whether it's fine to have 3 local funcs.
func bqMergeWithDatastore(c appengine.Context, newCGs map[string]*ClientGroup) {
	// Divide GetMulti and PutMulti operations into MaxDSReadPerQuery sized
	// operations to adhere with GAE limits.
	chunks := make([]*dsReadChunk, 0)
	var thisChunk *dsReadChunk
	// newChunk creates a new dsReadChunk as necessary
	newChunk := func() {
		thisChunk = &dsReadChunk{
			Keys: make([]*datastore.Key, 0, MaxDSReadPerQuery),
			CGs:  make([]*ClientGroup, 0, MaxDSReadPerQuery),
		}
		chunks = append(chunks, thisChunk)
	}
	newChunk()

	rttKey := datastore.NewKey(c, "string", "rtt", 0, nil)
	for cgStr, cg := range newCGs {
		thisChunk.Keys = append(thisChunk.Keys, datastore.NewKey(c, "ClientGroup", cgStr, 0, rttKey))
		thisChunk.CGs = append(thisChunk.CGs, cg)

		// Make sure read chunks are only as large as MaxDSReadPerQuery
		if len(thisChunk.CGs) == MaxDSReadPerQuery {
			newChunk()
		}
	}

	var oldCGs []ClientGroup
	var merr appengine.MultiError
	var err error

	keysToPut := make([]*datastore.Key, 0, MaxDSWritePerQuery)
	cgsToPut := make([]ClientGroup, 0, MaxDSWritePerQuery)
	totalPutN := 0

	// processPutQueue processes a queue of newly updated ClientGroups. This is
	// done so that MaxDSWritePerQuery no. of Puts can be done to reduce the
	// number of queries to datastore and therefore the time taken to Put all
	// changes to datastore.
	processPutQueue := func() {
		totalPutN += len(cgsToPut)
		c.Infof("rtt: Putting %v records into datastore. (Total: %d rows)", len(cgsToPut), totalPutN)

		_, err = datastore.PutMulti(c, keysToPut, cgsToPut)
		if err != nil {
			c.Errorf("rtt: bqMergeWithDatastore.datastore.PutMulti: %s", err)
		}
		keysToPut = make([]*datastore.Key, 0, MaxDSWritePerQuery)
		cgsToPut = make([]ClientGroup, 0, MaxDSWritePerQuery)
	}

	// processPutQueue places a newly updated ClientGroup in a queue. This queue
	// is later processed by processPutQueue.
	queueToPut := func(idx int, keys []*datastore.Key) {
		keysToPut = append(keysToPut, keys[idx])
		cgsToPut = append(cgsToPut, oldCGs[idx])

		// Write in MaxDSWritePerQuery chunks to adhere with GAE limits.
		if len(keysToPut) == MaxDSWritePerQuery {
			processPutQueue()
		}
	}

	// Process chunk by chunk
	for _, chunk := range chunks {
		oldCGs = make([]ClientGroup, len(chunk.CGs))
		err = datastore.GetMulti(c, chunk.Keys, oldCGs) // Get existing ClientGroup data

		switch err.(type) {
		case appengine.MultiError: // Multiple errors, deal with individually
			merr = err.(appengine.MultiError)
			for i, e := range merr { // Range over errors
				switch e {
				case datastore.ErrNoSuchEntity: // New entry
					oldCGs[i] = *chunk.CGs[i]
					queueToPut(i, chunk.Keys)
				case nil: // Entry exists, merge new data with old data
					// If for some reason data is corrupted and nil is returned
					if oldCGs[i].SiteRTTs == nil {
						oldCGs[i] = *chunk.CGs[i]
						queueToPut(i, chunk.Keys)
					} else {
						changed, err := MergeClientGroups(&oldCGs[i], chunk.CGs[i])
						if err != nil {
							c.Errorf("rtt: bqMergeWithDatastore.MergeClientGroups: %s", err)
						}
						if changed {
							queueToPut(i, chunk.Keys)
						}
					}
				default: // Other unknown error
					c.Errorf("rtt: bqMergeWithDatastore.datastore.GetMulti: %s", err)
				}
			}
		case nil: // No errors, data exists so merge with old data
			for i, _ := range oldCGs {
				changed, err := MergeClientGroups(&oldCGs[i], chunk.CGs[i])
				if err != nil {
					c.Errorf("rtt: bqMergeWithDatastore.MergeClientGroups: %s", err)
				}
				if changed {
					queueToPut(i, chunk.Keys)
				}
			}
		default: // Other unknown errors from GetMulti
			c.Errorf("rtt: bqMergeWithDatastore.datastore.GetMulti: %s", err)
		}
	}

	// Process remaining Put operations.
	if len(keysToPut) > 0 {
		processPutQueue()
	}

	c.Infof("rtt: Completed merging in %d rows from BigQuery.", len(newCGs))
}