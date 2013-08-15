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
	"appengine/urlfetch"
	"code.google.com/p/golog2bq/log2bq"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"net/http"
	"time"
)

func bqLogin(r *http.Request) (*http.Client, error) {
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

	client, err := transport.Client()
	if err != nil {
		return nil, err
	}
	return client, nil
}
