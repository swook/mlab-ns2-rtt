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
	"appengine/memcache"
	"appengine/urlfetch"
	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"errors"
	"net/http"
	"time"
)

const (
	dev_OAuthTokenKey = "dev_OAuthToken" // The key used to memcache oauth2 tokens for the dev server
)

// Login to BQ using goauth2. Note that the authentication URL is displayed in
// dev_appserver.py logs while an instance is running locally.
func bqLoginDev(r *http.Request) (*http.Client, error) {
	c := appengine.NewContext(r)
	code := r.FormValue("bq-code")

	// Get cached token from previous request
	var cachedToken *oauth.Token
	memcache.JSON.Get(c, dev_OAuthTokenKey, &cachedToken)

	// Set up a configuration.
	config := &oauth.Config{
		ClientId:     r.FormValue("bq-id"),
		ClientSecret: r.FormValue("bq-secret"),
		Scope:        bigquery.BigqueryScope,
		RedirectURL:  "oob",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
	}
	if config.ClientId == "" || config.ClientSecret == "" { // No ID or Secret provided
		return nil, errors.New("rtt: URL parameters 'bq-id' and 'bq-secret' required for oauth authentication to BigQuery.")
	}
	if code == "" && cachedToken == nil { // No Token provided and no token cached
		askForOAuthCode(c, config)
		return nil, errors.New("rtt: URL parameter 'bq-code' required for oauth authentication to BigQuery.")
	}

	var isOldToken bool
	if cachedToken != nil {
		// Check whether token expired
		isOldToken = cachedToken.Expiry.Before(time.Now())
		if isOldToken {
			c.Debugf("rtt: OAuth Token expired: %+v", cachedToken)
		} else {
			c.Debugf("rtt: OAuth Token expiring in: %v", cachedToken.Expiry.Sub(time.Now()))
		}
	}

	transport := &oauth.Transport{
		Config: config,
		Transport: &urlfetch.Transport{
			Context: c,
		},
	}

	if isOldToken || cachedToken == nil { // Token expired or no token cached
		token, err := transport.Exchange(code)
		if err != nil {
			c.Errorf("rtt: oauth.Transport.Exchange: %s", err)
			askForOAuthCode(c, config)
			return nil, err
		}
		transport.Token = token
		// memcache token for future requests while the server is running
		memcache.JSON.Set(c, &memcache.Item{
			Key:    dev_OAuthTokenKey,
			Object: token,
		})
	} else {
		transport.Token = cachedToken
	}

	client := transport.Client()
	return client, nil
}

func askForOAuthCode(c appengine.Context, config *oauth.Config) {
	url := config.AuthCodeURL("")
	c.Errorf("rtt: Visit this URL to get a code, then run again with URL parameter bq-code=YOUR_CODE\n%s\n", url)
}
