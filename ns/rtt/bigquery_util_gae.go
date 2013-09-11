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
)

// dsWriteChunk is a structure with which new ClientGroup lists can be split
// into lengths <= MaxDSWritePerQuery such that datastore.PutMulti works.
type dsReadChunk struct {
	Keys []*datastore.Key
	CGs  []*ClientGroup
}

// appendNewDSReadChunk appends a new dsReadChunk to an existing []*dsReadChunk
func appendNewDSReadChunk(chunks []*dsReadChunk) []*dsReadChunk {
	thisChunk := &dsReadChunk{
		Keys: make([]*datastore.Key, 0, MaxDSReadPerQuery),
		CGs:  make([]*ClientGroup, 0, MaxDSReadPerQuery),
	}
	return append(chunks, thisChunk)
}

// populateDSReadChunks divides GetMulti operations into MaxDSReadPerQuery sized
// operations to adhere with GAE limits for a given map[string]*ClientGroup.
func divideIntoDSReadChunks(c appengine.Context, newCGs map[string]*ClientGroup) []*dsReadChunk {
	chunks := make([]*dsReadChunk, 0)
	chunks = appendNewDSReadChunk(chunks) // Create initial dsReadChunk

	var thisChunk *dsReadChunk
	rttKey := datastore.NewKey(c, "string", "rtt", 0, nil) // Parent key for ClientGroup entities
	for cgStr, cg := range newCGs {
		// Add into chunk
		thisChunk.Keys = append(thisChunk.Keys, datastore.NewKey(c, "ClientGroup", cgStr, 0, rttKey))
		thisChunk.CGs = append(thisChunk.CGs, cg)

		// Make sure read chunks are only as large as MaxDSReadPerQuery.
		// Create new chunk if size reached.
		if len(thisChunk.CGs) == MaxDSReadPerQuery {
			chunks = appendNewDSReadChunk(chunks)
		}
	}
	return chunks
}

// addToPutQueue places a newly updated ClientGroup in a queue. This queue
// is later processed by processPutQueue.
func addToPutQueue(c appengine.Context, idx int, keys []*datastore.Key, keysToPut []*datastore.Key, cgsToPut []ClientGroup, oldCGs []ClientGroup, totalPutN int) ([]*datastore.Key, []ClientGroup, int) {
	keysToPut = append(keysToPut, keys[idx])
	cgsToPut = append(cgsToPut, oldCGs[idx])

	// Write in MaxDSWritePerQuery chunks to adhere with GAE limits.
	if len(keysToPut) == MaxDSWritePerQuery {
		keysToPut, cgsToPut, totalPutN = processPutQueue(c, keysToPut, cgsToPut, totalPutN)
	}
	return keysToPut, cgsToPut, totalPutN
}

// processPutQueue processes a queue of newly updated ClientGroups. This is
// done so that MaxDSWritePerQuery no. of Puts can be done to reduce the
// number of queries to datastore and therefore the time taken to Put all
// changes to datastore.
func processPutQueue(c appengine.Context, keysToPut []*datastore.Key, cgsToPut []ClientGroup, totalPutN int) ([]*datastore.Key, []ClientGroup, int) {
	totalPutN += len(cgsToPut)
	c.Infof("rtt: Putting %v records into datastore. (Total: %d rows)", len(cgsToPut), totalPutN)

	_, err := datastore.PutMulti(c, keysToPut, cgsToPut)
	if err != nil {
		c.Errorf("rtt.bqMergeWithDatastore:datastore.PutMulti: %s", err)
	}
	keysToPut = make([]*datastore.Key, 0, MaxDSWritePerQuery)
	cgsToPut = make([]ClientGroup, 0, MaxDSWritePerQuery)
	return keysToPut, cgsToPut, totalPutN
}
