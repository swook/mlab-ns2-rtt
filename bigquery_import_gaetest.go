// +build appengine

package rtt

import (
	"net/http"
)

const (
	URLBQInit = "/rtt/init"
)

func init() {
	http.HandleFunc(URLBQInit, bqinit)
}

func bqinit(w http.ResponseWriter, r *http.Request) {
	bqLoginDevPrepare(w, r)
}
