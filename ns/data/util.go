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

package data

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
)

// GetData returns a datastore.Get result and also caches the result into
// memcache.
func GetData(c appengine.Context, mcKey string, dsKey *datastore.Key, dst interface{}) error {
	err := mcGet(c, mcKey, dst)
	switch err {
	case memcache.ErrCacheMiss:
		if err := datastore.Get(c, dsKey, dst); err != nil {
			return err
		}
		mcSet(c, mcKey, dst)
		return nil
	case nil:
		return nil
	}
	return err
}

// QueryData returns a datastore.Query.GetAll result and also caches the result
// into memcache.
func QueryData(c appengine.Context, mcKey string, q *datastore.Query, dst interface{}) error {
	err := mcGet(c, mcKey, dst)
	switch err {
	case memcache.ErrCacheMiss:
		if _, err := q.GetAll(c, dst); err != nil {
			return err
		}
		mcSet(c, mcKey, dst)
		return nil
	case nil:
		return nil
	}
	return err
}

func mcGet(c appengine.Context, key string, dst interface{}) error {
	_, err := memcache.Gob.Get(c, key, dst)
	if err != nil {
		return err
	}
	return nil
}

func mcSet(c appengine.Context, key string, data interface{}) error {
	item := &memcache.Item{
		Key:    key,
		Object: data,
	}
	err := memcache.Gob.Set(c, item)
	if err != nil {
		return err
	}
	return nil
}
