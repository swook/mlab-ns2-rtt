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
	"appengine/memcache"
	"appengine/urlfetch"
	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/golog2bq/log2bq"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const (
	dev_OAuthIDKey     = "dev_OAuthID"
	dev_OAuthSecretKey = "dev_OAuthSecret"
	dev_OAuthCodeKey   = "dev_OAuthCode"
	dev_OAuthTokenKey  = "dev_OAuthToken" // The key used to memcache oauth2 tokens for the dev server
)

// bqInit logs in to bigquery using OAuth and returns a *bigquery.Service with
// which to make queries to bigquery.
func bqInit(r *http.Request) (*bigquery.Service, error) {
	var client *http.Client
	var err error
	if appengine.IsDevAppServer() {
		client, err = bqLoginDev(r)
	} else {
		client, err = bqLogin(r)
	}
	if err != nil {
		return nil, err
	}

	service, err := bigquery.New(client)
	return service, err
}

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

func bqLoginDevPrepare(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	id := r.FormValue("bq-id")
	secret := r.FormValue("bq-secret")
	code := r.FormValue("bq-code")

	transport := getBQOAuthTransport(c, id, secret)
	if code == "" {
		requestOAuthAuth(w, transport.Config)
	} else {
		token, err := transport.Exchange(code)
		if err != nil {
			requestOAuthAuth(w, transport.Config)
			return
		}

		memcache.JSON.Set(c, &memcache.Item{
			Key:    dev_OAuthIDKey,
			Object: id,
		})
		memcache.JSON.Set(c, &memcache.Item{
			Key:    dev_OAuthSecretKey,
			Object: secret,
		})
		memcache.JSON.Set(c, &memcache.Item{
			Key:    dev_OAuthCodeKey,
			Object: code,
		})
		memcache.JSON.Set(c, &memcache.Item{
			Key:    dev_OAuthTokenKey,
			Object: token,
		})
		fmt.Fprintf(w, "OAuth2 ID, Secret, Code, and Token for BigQuery, cached.")
	}
}

func requestOAuthAuth(w http.ResponseWriter, config *oauth.Config) {
	url := config.AuthCodeURL("")
	fmt.Fprintf(w, `<p>Please visit <a href="%s">this link</a> and add the code to the request URI with parameter 'bq-code'.</p>`, url)
}

// Login to BQ using goauth2. Note that the authentication URL is displayed in
// dev_appserver.py logs while an instance is running locally.
func bqLoginDev(r *http.Request) (*http.Client, error) {
	c := appengine.NewContext(r)

	// Get cached token from previous request
	var cachedID, cachedSecret, cachedCode string
	var cachedToken *oauth.Token
	memcache.JSON.Get(c, dev_OAuthIDKey, &cachedID)
	memcache.JSON.Get(c, dev_OAuthSecretKey, &cachedSecret)
	memcache.JSON.Get(c, dev_OAuthCodeKey, &cachedCode)
	memcache.JSON.Get(c, dev_OAuthTokenKey, &cachedToken)

	// Set up a configuration.
	if cachedID == "" || cachedSecret == "" || cachedCode == "" || cachedToken == nil {
		c.Debugf("%v %v %v %v", cachedID, cachedSecret, cachedCode, cachedToken)
		return nil, errors.New("rtt: Please visit /rtt/init to renew BigQuery OAuth authentication.")
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

	transport := getBQOAuthTransport(c, cachedID, cachedSecret)

	if isOldToken || cachedToken == nil { // Token expired or no token cached
		token, err := transport.Exchange(cachedCode)
		if err != nil {
			c.Errorf("rtt: oauth.Transport.Exchange: %s", err)
			c.Errorf("rtt: Please visit /rtt/init to renew BigQuery OAuth authentication.")
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

func getBQOAuthTransport(c appengine.Context, id, secret string) *oauth.Transport {
	config := &oauth.Config{
		ClientId:     id,
		ClientSecret: secret,
		Scope:        bigquery.BigqueryScope,
		RedirectURL:  "oob",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
	}
	return &oauth.Transport{
		Config: config,
		Transport: &urlfetch.Transport{
			Context: c,
		},
	}
}
