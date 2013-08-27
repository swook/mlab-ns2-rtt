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
