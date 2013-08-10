package rtt

import (
	"fmt"
	"net/http"
)

const (
	URLBQTestBuild = "/rtt/build"
	URLBQTestShow  = "/rtt/show"
)

func init() {
	http.HandleFunc(URLBQTestBuild, bqbuild)
	http.HandleFunc(URLBQTestShow, bqshow)
}

var RTTDB map[string]*ClientGroup

func bqbuild(w http.ResponseWriter, r *http.Request) {
	bqImportDaily(w, r)
	fmt.Fprintf(w, "<h1>%d ClientGroup-Server pairs processed.</h1>", len(RTTDB))
}

func bqshow(w http.ResponseWriter, r *http.Request) {
	for _, v := range RTTDB {
		fmt.Fprintf(w, "<p><b>ClientGroup: %s</b><br>", v.Prefix)
		for _, s := range v.SiteRTTs {
			fmt.Fprintf(w, "%fms to %s recorded at %s<br>", s.RTT, s.SiteID, s.LastUpdated)
		}
		fmt.Fprint(w, "</p>")
	}
}
