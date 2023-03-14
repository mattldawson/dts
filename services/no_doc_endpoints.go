//go:build !docs
// +build !docs

// This bypasses the generation of documentation endpoints.

package services

import (
	"github.com/gorilla/mux"
)

var HaveDocEndpoints bool = false

func AddDocEndpoints(r *mux.Router) {
}
