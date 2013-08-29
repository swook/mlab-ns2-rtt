// +build appengine

package rtt

import (
	"appengine"
	"appengine/datastore"
	"code.google.com/p/mlab-ns2/gae/ns/data"
	"fmt"
	"net/http"
	"time"
)

func init() {
	// http.HandleFunc("/tmp", convertSites)
}

func convertSites(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	q := datastore.NewQuery("Site").KeysOnly()
	keys, _ := q.GetAll(c, nil)

	oldSites := make([]oldSite, 0, len(keys))
	newKeys := make([]*datastore.Key, 0, len(keys))
	var os oldSite
	var ns data.Site
	var err error
	var ok bool
	for _, k := range keys {
		ns = data.Site{}
		err = datastore.Get(c, k, &ns)
		_, ok = err.(*datastore.ErrFieldMismatch)
		if ok {
			os = oldSite{}
			err = datastore.Get(c, k, &os)
			if err == nil {
				newKeys = append(newKeys, k)
				oldSites = append(oldSites, os)
			}
		}
	}

	newSites := make([]data.Site, len(oldSites))
	var us *data.Site
	for i, s := range oldSites {
		if s.Metro == nil {
			continue
		}
		us = &data.Site{}
		us.SiteID = s.SiteID
		us.City = s.City
		us.Country = s.Country
		us.Latitude = s.Latitude
		us.Longitude = s.Longitude
		us.Metro = s.Metro
		us.RegistrationTimestamp = s.RegistrationTimestamp
		us.When = s.When
		newSites[i] = *us
	}
	fmt.Fprintf(w, "Putting %d fixed Sites to datastore.\n", len(newSites))
	for i, s := range newSites {
		err = datastore.Delete(c, newKeys[i])
		if err != nil {
			fmt.Fprintln(w, err)
		}
		_, err = datastore.Put(c, newKeys[i], &s)
		if err != nil {
			fmt.Fprintln(w, err)
		}
		fmt.Fprintf(w, "Put fixed Site %s to datastore.\n", s.SiteID)
	}
}

type oldSite struct {
	SiteID                string    `datastore:"site_id"`
	City                  string    `datastore:"city"`
	Country               string    `datastore:"country"`
	Latitude              float64   `datastore:"latitude"`               // Latitude of the airport that uniquely identifies an M-Lab site.
	Longitude             float64   `datastore:"longitude"`              // Longitude of the airport that uniquely identifies an M-Lab site.
	Metro                 []string  `datastore:"metro"`                  // List of sites and metros, e.g., [ath, ath01].
	RegistrationTimestamp int64     `datastore:"registration_timestamp"` // Date representing the registration time (the first time a new site is added to mlab-ns).
	When                  time.Time `datastore:"when"`                   // Date representing the last modification time of this entity.
	Region                bool      `datastore:"region"`
	Timestamp             int64     `datastore:"timestamp"`
}
