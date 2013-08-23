// +build appengine

package rtt

import (
	"net"
	"net/http"
)

const (
	URLBQInit = "/rtt/init"
)

// SitesDB stores a map of site IDs to *Sites.
var SitesDB = make(map[string]*Site)

// A Site contains a set of Slivers
type Site struct {
	ID      string
	Slivers []*Sliver
}

// SliversDB stores a map of sliver IPs to *Sites.
var SliversDB = make(map[string]*Site)

// Sliver represents a server which runs within a parent Site
type Sliver struct {
	IP   net.IP
	Site *Site
}

func init() {
	http.HandleFunc(URLBQInit, bqinit)
}

func bqinit(w http.ResponseWriter, r *http.Request) {
	bqLoginDevPrepare(w, r)
}
