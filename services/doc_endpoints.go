//go:build docs
// +build docs

package services

import (
	"embed"
	"net/http"

	"github.com/gorilla/mux"
)

var HaveDocEndpoints bool = true

//go:embed docs
var docs embed.FS

func AddDocEndpoints(r *mux.Router) {
	docServer := http.FileServer(http.FS(docs))
	r.PathPrefix("/docs").Handler(docServer).Methods("GET")
}
