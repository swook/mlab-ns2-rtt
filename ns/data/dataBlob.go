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
	"appengine/blobstore"
	"appengine/datastore"
	"fmt"
	"io"
	"time"
)

// A FileBlob is used to keep track of data and configuration files that have
// been stored in the blobstore.
type FileBlob struct {
	Type         string            // Default is blob content type
	Name         string            // Default is blob filename
	BlobKey      appengine.BlobKey // Key for accessing the blob
	ContentType  string            // Internet media type
	CreationTime time.Time         // Time the blob was created
	Filename     string            // Original blob filename
	Size         int64             // Size of the blob in bytes
	Data         io.Reader         `datastore:"-"` // The blob data
}

// NewFileBlob creates a FileBlob based on a blobstore.BlobInfo.
func NewFileBlob(i *blobstore.BlobInfo) *FileBlob {
	return &FileBlob{
		Type:         i.ContentType,
		Name:         i.Filename,
		BlobKey:      i.BlobKey,
		ContentType:  i.ContentType,
		CreationTime: i.CreationTime,
		Filename:     i.Filename,
		Size:         i.Size,
	}
}

// Key creates a standard key string for a FileBlob based on the Type and Name.
func (fb *FileBlob) Key() string {
	return fmt.Sprintf("%s_%s", fb.Type, fb.Name)
}

// key creates a datastore.Key based on the FileBlob's string key.
func (fb *FileBlob) key(c appengine.Context) *datastore.Key {
	return datastore.NewKey(c, "FileBlob", fb.Key(), 0, nil)
}

// Save stores the FileBlob in the datastore.  Note that the actual data is
// still stored in the blobstore, but this is an indexable reference to that
// blob.
func (fb *FileBlob) Save(c appengine.Context) (*datastore.Key, error) {
	return datastore.Put(c, fb.key(c), fb)
}

// GetFileBlob retrieves a specific FileBlob from the datastore and opens an
// io.Reader for that blob.
func GetFileBlob(c appengine.Context, t, n string) (*FileBlob, error) {
	fb := &FileBlob{
		Type: t,
		Name: n,
	}
	err := datastore.Get(c, fb.key(c), fb)
	if err != nil {
		return fb, err
	}
	fb.Data = blobstore.NewReader(c, fb.BlobKey)
	return fb, nil
}
