package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/net/netutil"

	"dts/config"
	"dts/core"
)

// This type implements the TransferService interface, allowing file transfers
// from JGI (via the JGI Data Portal) to KBase via Globus.
type prototype struct {
	// Name of the service.
	Name string
	// Service version identifier.
	Version string
	// Port on which the service currently runs.
	Port int
	// Router for REST endpoints.
	Router *mux.Router
	// HTTP server.
	Server *http.Server
}

// This type encodes a JSON object for responding to root queries.
type RootResponse struct {
	Name          string `json:"name"`
	Version       string `json:"version"`
	Uptime        int    `json:"uptime"`
	Documentation string `json:"documentation,omitempty"`
}

// Handler method for root.
func (service *prototype) getRoot(w http.ResponseWriter,
	r *http.Request) {
	log.Printf("Querying root endpoint...")
	data := RootResponse{
		Name:    service.Name,
		Version: service.Version,
		Uptime:  int(core.Uptime())}
	if HaveDocEndpoints {
		data.Documentation = "/docs"
	}
	jsonData, _ := json.Marshal(data)
	writeJson(w, jsonData)
}

// type holding database metadata
type dbMetadata struct {
	Id           string `json:"id"`
	Name         string `json:"name"`
	Organization string `json:"organization"`
	URL          string `json:"url"`
}

// handler method for querying all databases
func (service *prototype) getDatabases(w http.ResponseWriter,
	r *http.Request) {
	log.Printf("Querying organizational databases...")
	dbs := make([]dbMetadata, 0)
	for dbName, db := range config.Databases {
		dbs = append(dbs, dbMetadata{
			Id:           dbName,
			Name:         db.Name,
			Organization: db.Organization,
			URL:          db.URL,
		})
	}
	// FIXME: sort by name
	jsonData, _ := json.Marshal(dbs)
	writeJson(w, jsonData)
}

// handler method for querying a single database for its metadata
func (service *prototype) getDatabase(w http.ResponseWriter,
	r *http.Request) {
	vars := mux.Vars(r)
	dbName := vars["db"]

	log.Printf("Querying database %s...", dbName)
	db, ok := config.Databases[dbName]
	if !ok {
		errStr := fmt.Sprintf("Database %s not found", dbName)
		log.Print(errStr)
		writeError(w, errStr, 404)
	} else {
		data, _ := json.Marshal(dbMetadata{
			Id:           dbName,
			Name:         db.Name,
			Organization: db.Organization,
			URL:          db.URL,
		})
		writeJson(w, data)
	}
}

// This helper translates an array of engines.SearchResults to a JSON object
// containing search results for the query (including the database name).
func (service *prototype) jsonFromSearchResults(dbName string,
	query string, results core.SearchResults) ([]byte, error) {

	data := ElasticSearchResponse{
		Database: dbName,
		Query:    query,
		Files:    results.Files,
	}

	return json.Marshal(data)
}

// handle ElasticSearch queries
func (service *prototype) searchDatabase(w http.ResponseWriter,
	r *http.Request) {

	// fetch search parameters
	dbName := r.FormValue("database")
	query := r.FormValue("query")

	// is the database valid?
	_, ok := config.Databases[dbName]
	if !ok {
		errStr := fmt.Sprintf("Database %s not found", dbName)
		log.Print(errStr)
		writeError(w, errStr, 404)
		return
	}

	// are we asked to return a subset of our results?
	pagination, err := extractPaginationParams(r)
	if err != nil {
		writeError(w, err.Error(), 400)
		return
	}

	log.Printf("Searching database %s for files...", dbName)
	db, err := core.NewDatabase(dbName)
	if err != nil {
		writeError(w, err.Error(), 404)
	}
	results, err := db.Search(query, pagination)
	if err != nil {
		writeError(w, err.Error(), 400)
	} else {
		// Return our results to the caller.
		jsonData, _ := service.jsonFromSearchResults(dbName, query, results)
		writeJson(w, jsonData)
	}
}

// Handler method for queueing a batch gene homology search.
func (service *prototype) createTransfer(w http.ResponseWriter,
	r *http.Request) {
	// TODO: implement!
	response := TransferResponse{
		Id: uuid.New(),
	}
	jsonData, _ := json.Marshal(response)
	writeJson(w, jsonData)
}

// Helper for extracting pagination parameters.
func extractPaginationParams(r *http.Request) (core.Pagination, error) {
	v := r.URL.Query()
	p := core.Pagination{Offset: 0, MaxNum: -1}
	offsetVal := v.Get("offset")
	if offsetVal != "" {
		var err error
		p.Offset, err = strconv.Atoi(offsetVal)
		if err != nil {
			err = fmt.Errorf("Error: Invalid results offset: %s", offsetVal)
			return p, err
		} else if p.Offset < 0 {
			err = fmt.Errorf("Error: Invalid results offset: %d", p.Offset)
			return p, err
		}
	}
	NVal := v.Get("limit")
	if NVal != "" {
		var err error
		p.MaxNum, err = strconv.Atoi(NVal)
		if err != nil {
			err = fmt.Errorf("Invalid results limit: %s", NVal)
			return p, err
		} else if p.MaxNum < 0 {
			err = fmt.Errorf("Invalid results limit: %d", p.MaxNum)
			return p, err
		}
	}
	return p, nil
}

// handler method for getting the status of a transfer
func (service *prototype) getTransferStatus(w http.ResponseWriter,
	r *http.Request) {

	// Extract the transfer ID from the request.
	vars := mux.Vars(r)
	xferId, err := uuid.Parse(vars["id"])
	if err != nil {
		errStr := fmt.Sprintf("Invalid transfer ID: %s", xferId)
		writeError(w, errStr, 400)
		return
	}

	// Fetch the status for the job.
	// TODO: implement this
	status := TransferStatusResponse{Id: xferId.String(), Status: "staging"}
	jsonData, _ := json.Marshal(status)
	writeJson(w, jsonData)
}

// Constructs a prototype file transfer service given our configuration
func NewDTSPrototype() (TransferService, error) {
	service := new(prototype)
	service.Name = "DTS prototype"
	service.Version = core.Version
	service.Port = -1

	// Set up routing.
	r := mux.NewRouter()
	r.HandleFunc("/", service.getRoot).Methods("GET")

	// Serve documentation endpoints.
	AddDocEndpoints(r)

	// API calls are routed through /api.
	api := r.PathPrefix("/api").Subrouter()
	api.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	// Version 1.
	api_v1 := api.PathPrefix("/v1").Subrouter()
	api_v1.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	api_v1.HandleFunc("/databases", service.getDatabases).Methods("GET")
	api_v1.HandleFunc("/databases/{db}", service.getDatabase).Methods("GET")
	api_v1.HandleFunc("/files", service.searchDatabase).Methods("GET")
	api_v1.HandleFunc("/transfers", service.createTransfer).Methods("POST")
	api_v1.HandleFunc("/transfers/{id}", service.getTransferStatus).Methods("GET")
	// TODO: add DELETE mechanism for /transfers/{id}
	service.Router = r

	return service, nil
}

// starts the prototype data transfer service
func (service *prototype) Start(port int) error {
	log.Printf("Starting %s service on port %d...", service.Name, port)
	log.Printf("(Accepting up to %d connections)", config.Service.MaxConnections)

	// Set the port.
	service.Port = port

	// Create a listener that limits the number of incoming connections.
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer listener.Close()
	listener = netutil.LimitListener(listener, config.Service.MaxConnections)

	// Start the server.
	service.Server = &http.Server{
		Handler: service.Router}
	err = service.Server.Serve(listener)

	// We don't report the server closing as an error.
	if err != http.ErrServerClosed {
		return err
	} else {
		return nil
	}
}

// gracefully shuts down the service without interrupting active connections
func (service *prototype) Shutdown(ctx context.Context) error {
	err := service.Server.Shutdown(ctx)
	return err
}

// Closes down the service, freeing all resources.
func (service *prototype) Close() {
	service.Server.Close()
}
