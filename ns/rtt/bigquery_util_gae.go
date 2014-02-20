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
	"appengine/memcache"
	"appengine/taskqueue"
	"fmt"
	"net/url"
	"time"
)

const (
	MaxDSReadPerQuery  = 1000
	MaxDSWritePerQuery = 300
)

// dsReadChunk is a structure with which new ClientGroup lists can be split into
// lengths <= MaxDSReadPerQuery such that datastore.GetMulti works.
type dsReadChunk struct {
	keys []*datastore.Key
	cgs  []*ClientGroup
}

// len returns the length of the slice *dsReadChunk.keys.
func (c *dsReadChunk) len() int {
	return len(c.keys)
}

// newDSReadChunk returns a new *dsReadChunk with the keys and cgs slices made.
func newDSReadChunk() *dsReadChunk {
	return &dsReadChunk{
		keys: make([]*datastore.Key, 0, MaxDSReadPerQuery),
		cgs:  make([]*ClientGroup, 0, MaxDSReadPerQuery),
	}
}

// divideIntoDSReadChunks divides GetMulti operations into MaxDSReadPerQuery
// sized operations to adhere with GAE limits for a given map[string]*ClientGroup.
func divideIntoDSReadChunks(c appengine.Context, newcgs map[string]*ClientGroup) []*dsReadChunk {
	chunks := make([]*dsReadChunk, 0)
	chunk := newDSReadChunk()

	parentKey := DatastoreParentKey(c)
	for cgStr, cg := range newcgs {
		// Add into chunk
		chunk.keys = append(chunk.keys, datastore.NewKey(c, "ClientGroup", cgStr, 0, parentKey))
		chunk.cgs = append(chunk.cgs, cg)

		// Make sure read chunks are only as large as MaxDSReadPerQuery.
		// Create new chunk if size reached.
		if chunk.len() == MaxDSReadPerQuery {
			chunks = append(chunks, chunk)
			chunk = newDSReadChunk()
		}
	}
	return chunks
}

// dsWriteChunk is a structure with which new ClientGroup lists can be split
// into lengths <= MaxDSWritePerQuery such that datastore.PutMulti works.
type dsWriteChunk struct {
	keys []*datastore.Key
	cgs  []ClientGroup
}

// len returns the length of the slice *dsWriteChunk.keys.
func (c *dsWriteChunk) len() int {
	return len(c.keys)
}

// newDSWriteChunk returns a new *dsWriteChunk with the keys and cgs slices made.
func newDSWriteChunk() *dsWriteChunk {
	return &dsWriteChunk{
		keys: make([]*datastore.Key, 0, MaxDSWritePerQuery),
		cgs:  make([]ClientGroup, 0, MaxDSWritePerQuery),
	}
}

// putQueueRequest keeps track of a queue for datastore.PutMulti requests, as
// well as the total number of Puts done.
type putQueueRequest struct {
	key   *datastore.Key
	cg    *ClientGroup
	queue *dsWriteChunk
	putN  int
}

// add places a newly updated ClientGroup in a PutMulti queue. This queue is
// later processed by putQueueRequest.process.
func (r *putQueueRequest) add(c appengine.Context, dateStr string, k *datastore.Key, cg *ClientGroup) {
	if r.queue == nil || r.queue.keys == nil {
		r.queue = newDSWriteChunk()
	}

	r.queue.keys = append(r.queue.keys, k)
	r.queue.cgs = append(r.queue.cgs, *cg)

	if r.queue.len() == MaxDSWritePerQuery {
		r.process(c, dateStr)
	}
}

// process processes a queue of newly updated ClientGroups. This is done so that
// MaxDSWritePerQuery no. of Puts can be done to reduce the number of queries to
// datastore and therefore the time taken to Put all changes to datastore.
func (r *putQueueRequest) process(c appengine.Context, dateStr string) {
	if r.queue == nil || r.queue.len() == 0 { // Don't process further if nothing to process
		return
	}
	n := r.queue.len()

	r.putN += n
	c.Infof("rtt: Submitting put tasks for %v records. (Total: %d rows)", n, r.putN)

	addTaskClientGroupPut(c, dateStr, r.queue.cgs)
	r.queue = newDSWriteChunk()
}

// addTaskClientGroupPut receives a list of ClientGroups to put into datastore
// and stores it temporarily into memcache. It then submits the key as a
// taskqueue task.
func addTaskClientGroupPut(c appengine.Context, dateStr string, cgs []ClientGroup) {
	// Create unique key for memcache
	key := cgMemcachePutKey()

	// Store CGs into memcache
	item := &memcache.Item{
		Key:    key,
		Object: cgs,
	}
	if err := memcache.Gob.Set(c, item); err != nil {
		c.Errorf("rtt.addTaskClientGroupPut:memcache.Set: %s", err)
		return
	}

	// Submit taskqueue task
	values := make(url.Values)
	values.Add(FormKeyPutKey, key)
	values.Add(FormKeyImportDate, dateStr)
	task := taskqueue.NewPOSTTask(URLTaskImportPut, values)
	_, err := taskqueue.Add(c, task, TaskQueueNameImportPut)
	if err != nil {
		c.Errorf("rtt.addTaskClientGroupPut:taskqueue.Add: %s", err)
		return
	}
}

// DatastoreParentKey returns a datastore key to use as a parent key for rtt
// related datastore entries.
func DatastoreParentKey(c appengine.Context) *datastore.Key {
	return datastore.NewKey(c, "string", "rtt", 0, nil)
}

// cgMemcachePutKey generates a memcache key string for use in Put operations
// when []ClientGroup is cached into memcache.
func cgMemcachePutKey() string {
	ns := time.Now().UnixNano()
	return fmt.Sprintf("rtt:bqImport:Put:%d", ns)
}
