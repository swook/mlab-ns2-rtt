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
	"net"
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
