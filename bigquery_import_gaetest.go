// +build appengine

package rtt

import (
	"fmt"
	"net/http"
)

const (
	URLBQTestBuild = "/rtt/build"
	URLBQInit      = "/rtt/init"
)

func init() {
	http.HandleFunc(URLBQTestBuild, bqbuild)
	http.HandleFunc(URLBQInit, bqinit)
}

func bqbuild(w http.ResponseWriter, r *http.Request) {
	bqImportDaily(w, r)
	fmt.Fprintln(w, "Done")
}

func bqinit(w http.ResponseWriter, r *http.Request) {
	bqLoginDevPrepare(w, r)
}
