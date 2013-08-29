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
	"reflect"
	"testing"
	"time"
)

var simplifyBQResponseTests = []struct {
	in  []*bigquery.TableRow
	out bqRows
}{
	{
		[]*bigquery.TableRow{
			&bigquery.TableRow{
				F: []*bigquery.TableCell{
					&bigquery.TableCell{interface{}("123")},
					&bigquery.TableCell{interface{}("1.2.3.4")},
					&bigquery.TableCell{interface{}("5.6.7.8")},
					&bigquery.TableCell{interface{}("3.21")},
				},
			},
			&bigquery.TableRow{
				F: []*bigquery.TableCell{ // Row with defective data is ignored.
					&bigquery.TableCell{interface{}("7.89")}, // Needs to be int
					&bigquery.TableCell{interface{}("7.8.9.0")},
					&bigquery.TableCell{interface{}("1.2.3.4")},
					&bigquery.TableCell{interface{}("123")},
				},
			},
			&bigquery.TableRow{
				F: []*bigquery.TableCell{
					&bigquery.TableCell{interface{}("456")},
					&bigquery.TableCell{interface{}("9.0.1.2")},
					&bigquery.TableCell{interface{}("3.4.5.6")},
					&bigquery.TableCell{interface{}("12.4")},
				},
			},
		},
		bqRows{
			&bqRow{
				time.Unix(123, 0),
				net.ParseIP("1.2.3.4"),
				net.ParseIP("5.6.7.8"),
				3.21,
			},
			&bqRow{
				time.Unix(456, 0),
				net.ParseIP("9.0.1.2"),
				net.ParseIP("3.4.5.6"),
				12.4,
			},
		},
	},
}

func TestSimplifyBQResponse(t *testing.T) {
	var out bqRows
	for i, tt := range simplifyBQResponseTests {
		out = simplifyBQResponse(tt.in)
		if !reflect.DeepEqual(tt.out, out) {
			t.Fatalf("Error in index %d of simplifyBQResponseTests. Expected output not attained.", i)
		}
	}
}

var makeMapIPStrToSiteIDTests = []struct {
	in  []*data.SliverTool
	out map[string]string
}{
	{
		[]*data.SliverTool{
			&data.SliverTool{SiteID: "ams01", SliverIPv4: "213.244.128.164"},
			&data.SliverTool{SiteID: "ams02", SliverIPv4: "72.26.217.75", SliverIPv6: "2001:48c8:7::75"},
			&data.SliverTool{SiteID: "lca01", SliverIPv4: "82.116.199.38"},
			&data.SliverTool{SiteID: "lga01", SliverIPv4: "74.63.50.43", SliverIPv6: "2001:48c8:5:f::43"},
		},
		map[string]string{
			"213.244.128.164":   "ams01",
			"72.26.217.75":      "ams02",
			"2001:48c8:7::75":   "ams02",
			"82.116.199.38":     "lca01",
			"74.63.50.43":       "lga01",
			"2001:48c8:5:f::43": "lga01",
		},
	},
}

func TestMakeMapIPStrtoSiteID(t *testing.T) {
	var out map[string]string
	for i, tt := range makeMapIPStrToSiteIDTests {
		out = makeMapIPStrToSiteID(tt.in)
		if !reflect.DeepEqual(tt.out, out) {
			t.Fatalf("Error in index %d of makeMapIPStrToSiteIDTests. Expected output not attained.", i)
		}
	}
}

var bqMergeIntoClientGroupsTests = []struct {
	in  bqRows
	out map[string]*ClientGroup
}{
	{
		bqRows{
			&bqRow{
				time.Unix(1376828118, 0),
				net.ParseIP("74.63.50.43"),
				net.ParseIP("154.54.36.18"),
				761.5423380533854,
			},
			&bqRow{ // Test sorting of SiteRTTs
				time.Unix(1376828646, 0),
				net.ParseIP("82.116.199.38"),
				net.ParseIP("154.54.39.18"),
				62.007999420166016,
			},
			&bqRow{
				time.Unix(1376828891, 0),
				net.ParseIP("82.116.199.38"),
				net.ParseIP("90.185.4.231"),
				88.22200012207031,
			},
			&bqRow{
				time.Unix(1376828193, 0),
				net.ParseIP("74.63.50.43"),
				net.ParseIP("24.164.163.78"),
				38.31500116984049,
			},
			&bqRow{ // Test merging of existing SiteRTT
				time.Unix(1376828167, 0),
				net.ParseIP("74.63.50.43"),
				net.ParseIP("24.164.160.17"),
				7.705666700998942,
			},
		},
		map[string]*ClientGroup{
			"154.54.36.0": &ClientGroup{
				net.ParseIP("154.54.36.0").To16(),
				SiteRTTs{
					SiteRTT{
						"lca01",
						62.007999420166016,
						time.Unix(1376828646, 0),
					},
					SiteRTT{
						"lga01",
						761.5423380533854,
						time.Unix(1376828118, 0),
					},
				},
			},
			"90.185.4.0": &ClientGroup{
				net.ParseIP("90.185.4.0").To16(),
				SiteRTTs{
					SiteRTT{
						"lca01",
						88.22200012207031,
						time.Unix(1376828891, 0),
					},
				},
			},
			"24.164.160.0": &ClientGroup{
				net.ParseIP("24.164.160.0").To16(),
				SiteRTTs{
					SiteRTT{
						"lga01",
						7.705666700998942,
						time.Unix(1376828167, 0),
					},
				},
			},
		},
	},
}

func TestBQMergeIntoClientGroups(t *testing.T) {
	var out map[string]*ClientGroup
	for i, tt := range bqMergeIntoClientGroupsTests {
		out = make(map[string]*ClientGroup)
		bqMergeIntoClientGroups(tt.in, makeMapIPStrToSiteIDTests[0].out, out)

		// Make all ClientGroup.Prefix 16 bytes long to allow for reflect.DeepEqual comparison.
		for _, cg := range out {
			cg.Prefix = net.IP(cg.Prefix).To16()
		}

		if !reflect.DeepEqual(tt.out, out) {
			t.Fatalf("Error in index %d of bqMergeIntoClientGroups. Expected output not attained.", i)
		}
	}
}
