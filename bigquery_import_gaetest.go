// +build appengine

package rtt

import (
	"fmt"
	"net/http"
)

const (
	URLBQTestBuild = "/rtt/build"
)

func init() {
	http.HandleFunc(URLBQTestBuild, bqbuild)
}

func bqbuild(w http.ResponseWriter, r *http.Request) {
	bqImportDaily(w, r)
	fmt.Fprintln(w, "Done")
}
