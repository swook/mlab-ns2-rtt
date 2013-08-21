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

// The data package provides the mlab-ns2 datastore sturcture.
package data

import (
	"fmt"
	"time"
)

//TODO: only index the columns that are needed
//TODO: add json tags

type SliverTool struct {
	ToolID                 string    `datastore:"tool_id"`
	SliceID                string    `datastore:"slice_id"`
	SiteID                 string    `datastore:"site_id"`
	ServerID               string    `datastore:"server_id"`
	ServerPort             int64     `datastore:"server_port"`
	HTTPPort               int64     `datastore:"http_port"` // For web-based tools, this is used to build the URL the client is redirected to: http://fqdn[ipv4|ipv6]:http_port
	FQDN                   string    `datastore:"fqdn"`
	SliverIPv4             []byte    `datastore:"sliver_ipv4"`
	SliverIPv6             []byte    `datastore:"sliver_ipv6"`
	StatusIPv4             bool      `datastore:"status_ipv4"`
	StatusIPv6             bool      `datastore:"status_ipv6"`
	UpdateRequestTimestamp time.Time `datastore:"update_request_timestamp"` // To avoid an additional lookup in the datastore
	Latitude               float64   `datastore:"latitude"`                 // To avoid an additional lookup in the datastore
	Longitude              float64   `datastore:"longitude"`                // To avoid an additional lookup in the datastore
	City                   string    `datastore:"city"`                     // To avoid an additional lookup in the datastore
	Country                string    `datastore:"country"`                  // To avoid an additional lookup in the datastore
	When                   time.Time `datastore:"when"`                     // Date representing the last modification time of this entity.
}

type Site struct {
	SiteID                string    `datastore:"site_id"`
	City                  string    `datastore:"city"`
	Country               string    `datastore:"country"`
	Latitude              float64   `datastore:"latitude"`               // Latitude of the airport that uniquely identifies an M-Lab site.
	Longitude             float64   `datastore:"longitude"`              // Longitude of the airport that uniquely identifies an M-Lab site.
	Metro                 []string  `datastore:"metro"`                  // List of sites and metros, e.g., [ath, ath01].
	RegistrationTimestamp time.Time `datastore:"registration_timestamp"` // Date representing the registration time (the first time a new site is added to mlab-ns).
	When                  time.Time `datastore:"when"`                   // Date representing the last modification time of this entity.
}

// MMLocation is a format that comes from pre-processed geolocation data.  It is
// intended to be used with the iptrie location map.
//
// The utility takes maximind data as input, pre-processes the data so that it
// has just the amount of detail that we need and produces a binary file that is
// uploaded to the GAE instance.  On upload the handler will copy it into the
// blobstore and update the datastore with this format.
//
//TODO: reference to the location map compression utility.
//TODO: IPv6 addresses are truncated to a /64 before inclusion?
type MMLocation struct {
	RangeStart int64 // first IP address in the block
	RangeEnd   int64 // last IP address in the block
	Latitude   int   // latitude rounded to the nearest integer
	Longitude  int   // longitude rounded to the nearest integer
}

//XXX deprecated
type MaxmindCityLocation struct {
	LocationID string    `datastore:"location_id"`
	Country    string    `datastore:"country"`
	Region     string    `datastore:"region"`
	City       string    `datastore:"city"`
	Latitude   float64   `datastore:"latitude"`
	Longitude  float64   `datastore:"longitude"`
	When       time.Time `datastore:"when"`
}

//XXX deprecated
type MaxmindCityBlock struct {
	StartIPNum int64     `datastore:"start_ip_num"`
	EndIPNum   int64     `datastore:"end_ip_num"`
	LocationID string    `datastore:"location_id"`
	When       time.Time `datastore:"when"`
}

//XXX deprecated
type MaxmindCityBlockv6 struct {
	StartIPNum int64     `datastore:"start_ip_num"`
	EndIPNum   int64     `datastore:"end_ip_num"`
	Country    string    `datastore:"country"`
	Latitude   float64   `datastore:"latitude"`
	Longitude  float64   `datastore:"longitude"`
	When       time.Time `datastore:"when"`
}

//XXX deprecated
type CountryCode struct {
	Name        string    `datastore:"name"`
	Alpha2Code  string    `datastore:"alpha2_code"`
	Alpha3Code  string    `datastore:"alpha3_code"`
	NumericCode int64     `datastore:"numeric_code"`
	Latitude    float64   `datastore:"latitude"`
	Longitude   float64   `datastore:"longitude"`
	When        time.Time `datastore:"when"`
}

type EncryptionKey struct {
	KeyID         string `datastore:"key_id"`         // Name of the key (by default is 'admin').
	EncryptionKey []byte `datastore:"encryption_key"` // 16 bytes encryption key (AES).
}

type Slice struct {
	SliceID string `datastore:"slice_id"`
	ToolID  string `datastore:"tool_id"`
}

type Tool struct {
	SliceID  string `datastore:"slice_id"`
	ToolID   string `datastore:"tool_id"`
	HTTPPort int64  `datastore:"http_port"`
}

//TODO(gavaletz): generalize this to credentials?
type Nagios struct {
	KeyID    string `datastore:"key_id"`
	Username string `datastore:"username"`
	Password string `datastore:"password"`
	URL      string `datastore:"url"`
}

type Ping struct {
	Latitude      float64   `datastore:"latitude"`
	Longitude     float64   `datastore:"longitude"`
	ToolID        string    `datastore:"tool_id"`
	AddressFamily string    `datastore:"address_family"`
	Time          time.Time `datastore:"time"`
}

func GetSliverToolID(toolID, sliceID, serverID, siteID string) string {
	return fmt.Sprintf("%s-%s-%s-%s", toolID, sliceID, serverID, siteID)
}
