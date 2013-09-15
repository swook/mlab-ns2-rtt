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

package geo

import (
	"appengine"
	"appengine/datastore"
	"fmt"
	"io"
	"net/http"
	"strings"

	"code.google.com/p/mlab-ns2/gae/ns/data"
)

const (
	maxLat                = 180
	maxLon                = 360
	processMaxMindIPv4URL = "/admin/processMaxMindIPv4"
)

func init() {
	http.HandleFunc(processMaxMindIPv4URL, processMaxMindIPv4)
}

func byteTouint32(bytes []byte) uint32 {
	return (uint32(bytes[3]) | uint32(bytes[2])<<8 | uint32(bytes[1])<<16 | uint32(bytes[0])<<24)
}

// processMaxMindIPv4 processes a binary maxmind file with
// ipranges and custom location ids converted from uint32
// values to bytes.
func processMaxMindIPv4(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	blobInfo := r.FormValue("blobInfo")
	splitBlob := strings.Split(blobInfo, "-")
	if len(splitBlob) < 2 {
		fmt.Fprintf(w, "Provide type and name of file as in blobstore: ?blobInfo=fileType-fileName")
		return
	}

	f, err := data.GetFileBlob(c, splitBlob[0], splitBlob[1])
	if err != nil {
		c.Errorf("processMaxMindIPv4: GetFileBlob(%s-%s) err = %v", splitBlob[0], splitBlob[1], err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	reader := f.Data
	bytes := make([]byte, 4)
	for {
		// read start
		_, err = reader.Read(bytes)
		if err == io.EOF {
			break
		}
		mEntry := &data.MMLocation{
			RangeStart: int64(byteTouint32(bytes)),
		}

		// read end
		_, err = reader.Read(bytes)
		if err == io.EOF {
			c.Errorf("processMaxMindIPv4: Error in file format, %s-$s:", splitBlob[0], splitBlob[1])
			break
		}
		mEntry.RangeEnd = int64(byteTouint32(bytes))

		// read locid
		_, err = reader.Read(bytes)
		if err == io.EOF {
			c.Errorf("processMaxMindIPv4: Error in file format, %s-$s:", splitBlob[0], splitBlob[1])
			break
		}
		locid := int(byteTouint32(bytes))
		mEntry.Latitude = locid/1000 - maxLat/2
		mEntry.Longitude = locid%1000 - maxLon/2

		mKey := datastore.NewKey(c, "MMLocation", "", mEntry.RangeStart, nil)
		_, err := datastore.Put(c, mKey, mEntry)
		//TODO: Buffer and use PutMulti
		if err != nil {
			c.Errorf("processMaxMindIPv4:datastore.Put err = %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
