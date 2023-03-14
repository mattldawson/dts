package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unsafe" // for Sizeof

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

// This helper returns the index of the given namespace within the service, or
// -1 if the namespace is not found within the service.
func (service *prototype) findDatabase(database string) int {
	index := 0
	for index < len(service.Databases) {
		if service.Databases[index].Id == database {
			break
		}
		index++
	}
	if index >= len(service.Databases) {
		index = -1
	}
	return index
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

// handler method for querying all databases
func (service *prototype) getDatabases(w http.ResponseWriter,
	r *http.Request) {
	log.Printf("Querying organizational databases...")
	jsonData, _ := json.Marshal(service.Databases)
	writeJson(w, jsonData)
}

// handler method for querying a single database for its metadata
func (service *prototype) getDatabase(w http.ResponseWriter,
	r *http.Request) {
	vars := mux.Vars(r)
	database := vars["db"]

	log.Printf("Querying database %s...", database)
	index := service.findDatabase(database)
	if index < 0 {
		errStr := fmt.Sprintf("Database %s not found", database)
		log.Print(errStr)
		writeError(w, errStr, 404)
	} else {
		data, _ := json.Marshal(service.Databases[index])
		writeJson(w, data)
	}
}

// This helper translates an array of engines.SearchResults to a JSON object
// containing search results for the query (including the namespace).
func (service *prototype) jsonFromSearchResults(request SearchRequest,
	results []engines.SearchResult,
	includeQuerySeqs bool) ([]byte, error) {

	var data SearchResultsResponse

	// Identify the engine and the database for the given namespace.
	namespace := request.Namespace
	var nsIndex int = 0
	for i, ns := range service.Namespaces {
		if namespace == ns.Id {
			nsIndex = i
			break
		}
	}
	data.Engine = service.Databases[nsIndex].Engine
	data.Namespaces = make([]core.Namespace, 1)
	data.Namespaces[0] = service.Namespaces[nsIndex]

	// Include query sequences if requested
	if includeQuerySeqs {
		data.QuerySequences = make([]core.Sequence, len(request.Sequences))
		copy(data.QuerySequences, request.Sequences)
	}

	// Aggregate search results.
	data.Alignments = make([]AlignmentResponse, len(results))
	for i, r := range results {
		data.Alignments[i].QueryAlignment = r.QueryAlignment
		data.Alignments[i].TargetAlignment = r.TargetAlignment
		data.Alignments[i].BitScore = r.BitScore
		data.Alignments[i].EValue = r.EValue

		// Compute the BLAST-style percent identity, which is the number of matches
		// divided by the total length of the aligned portion of the query sequence.
		data.Alignments[i].PercentIdentity =
			100.0 * float64(r.Matches) / float64(r.QueryAlignment.Length)
	}

	return json.Marshal(data)
}

// Returns the minimum of integers a and b.
func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

// Helper for extracting and validating a SearchRequest from an http.Request.
func getSearchRequest(r *http.Request) (SearchRequest, error) {
	var req SearchRequest

	// Is this a JSON request?
	if r.Header.Get("Content-Type") != "application/json" {
		err := errors.New("Request content type must be \"application/json\".")
		return req, err
	}

	// Read the request body.
	r_body, err := io.ReadAll(r.Body)
	if err == nil {
		err = json.Unmarshal(r_body, &req)
		if err == nil {
			for i, seq := range req.Sequences {
				err = seq.Validate()
				if err != nil {
					err = fmt.Errorf("Sequence %d is invalid: %s", i, err.Error())
				}
			}
		}
	}
	if err != nil {
		return req, err
	}

	// Extract the namespace from the request.
	vars := mux.Vars(r)
	req.Namespace = vars["ns"]

	// Set defaults in place of zero values.
	if req.Params.EValue == 0.0 {
		req.Params.EValue = 1.0
	}
	if req.Params.MaxHits == 0 {
		req.Params.MaxHits = -1
	}
	if req.Limit == 0 {
		req.Limit = -1
	}

	// Validate the request.
	if len(req.Sequences) == 0 {
		err = errors.New("No sequences found in request")
	} else if req.Params.EValue <= 0.0 || req.Params.EValue > 1.0 {
		err = fmt.Errorf("Invalid E-value: %g (must be within (0, 1])",
			req.Params.EValue)
	} else if req.Params.MaxHits < -1 {
		err = fmt.Errorf("Invalid maxHits: %d (must be positive)",
			req.Params.MaxHits)
	} else if req.Offset < 0 {
		err = fmt.Errorf("Invalid offset parameter: %d",
			req.Offset)
	}
	if err != nil {
		return req, err
	}

	// Validate the request's query sequences.
	for i, seq := range req.Sequences {
		if seq.Id == "" {
			err = fmt.Errorf("Query sequence %d has no ID.", i)
		} else if seq.Data == "" {
			err = fmt.Errorf("Query sequence %d has no data.", i)
		}
	}

	return req, err
}

// Handler method for interactive gene homology search.
func (service *prototype) searchNamespace(w http.ResponseWriter,
	r *http.Request) {

	// Parse the request
	request, err := getSearchRequest(r)
	if err != nil {
		writeError(w, err.Error(), 400)
		return
	}

	// Are there too many sequences in the query?
	numSequences := len(request.Sequences)
	if numSequences > config.Interactive.MaxSequences {
		errStr := fmt.Sprintf("Too many sequences (%d given, %d allowed).",
			numSequences, config.Interactive.MaxSequences)
		writeError(w, errStr, 400)
		return
	}

	// Find the database for the namespace.
	index := 0
	for index < len(service.Namespaces) {
		if service.Namespaces[index].Id == request.Namespace {
			break
		}
		index++
	}
	if index == len(service.Namespaces) {
		errStr := fmt.Sprintf("Namespace %s not found", request.Namespace)
		writeError(w, errStr, 404)
		return
	}

	log.Printf("Searching namespace %s for alignments...", request.Namespace)

	// Check the cache for each of the sequences in this request.
	cachedResults := make(map[string][]engines.SearchResult)
	uncachedSequences := make([]core.Sequence, 0)
	for _, seq := range request.Sequences {
		results, err := service.Cache.Fetch(request.Namespace, seq,
			request.Params, request.Offset, request.Limit)
		if err != nil {
			writeError(w, err.Error(), 500)
			return
		}
		if results != nil {
			cachedResults[seq.Id] = results
		} else {
			uncachedSequences = append(uncachedSequences, seq)
		}
	}

	// If all the sequences in the request were found in the cache, we're
	// finished.
	if len(uncachedSequences) == 0 {
		if len(request.Sequences) == 1 {
			log.Printf("Found %d cached alignments for sequence %s.",
				len(cachedResults[request.Sequences[0].Id]), request.Sequences[0].Id)
		} else {
			log.Printf("Found cached results for all requested sequences.")
		}
		allResults := make([]engines.SearchResult, 0)
		for _, results := range cachedResults {
			for _, result := range results {
				allResults = append(allResults, result)
			}
		}
		jsonData, _ := service.jsonFromSearchResults(request, allResults, false)

		// Return the results to the caller.
		writeJson(w, jsonData)
		return
	} else {
		// These are the sequences we need to align.
		request.Sequences = uncachedSequences
	}

	// Find the database for this namespace.
	db := service.Databases[index]

	// Retrieve the engine to use for the search.
	engine := service.Engines[db.Engine]

	// If we haven't set up our worker pool yet, do so here.
	if service.InteractivePool == nil {
		// The total size of the interactive and batch pools must be maxProcesses.
		size := config.Service.MaxProcesses - config.Batch.MaxProcesses
		log.Printf("Initializing interactive worker pool (%d workers)...", size)
		service.InteractivePool = tunny.NewFunc(size,
			func(input interface{}) interface{} {
				in := input.(workInput)
				var results []engines.SearchResult
				err := engine.Validate(in.Database)
				if err == nil {
					results, err = in.Engine.Search(in.Database,
						in.Request.Sequences,
						in.Request.Params)
				}
				output := workOutput{
					Results: results,
					Error:   err}
				return output
			})
	}

	// Create a diagnostics entry for this request.
	diags := SearchServiceDiagRecord{
		Namespace:    request.Namespace,
		NumSequences: len(request.Sequences),
		Interactive:  true,
		Completed:    false,
	}
	diagIndex := service.Diags.Record(diags)

	// Call the search engine and obtain the results of its search.
	input := workInput{
		Engine:    engine,
		Database:  db,
		Request:   request,
		DiagIndex: diagIndex,
	}
	t1 := time.Now()
	output := service.InteractivePool.Process(input).(workOutput)
	t2 := time.Now()
	if output.Error != nil {
		errStr := fmt.Sprintf("Encountered an error searching the database: %s",
			output.Error.Error())
		writeError(w, errStr, 500)
		return
	}

	// Update the diagnostic record for this query.
	var size int
	for _, result := range output.Results {
		size += int(unsafe.Sizeof(result))
	}
	service.Diags.Update(diagIndex, t2.Sub(t1), size)

	// Cache each of the sets of new results for their sequences.
	for _, seq := range uncachedSequences {
		// Fish the results for this sequence out of the search output.
		seqResults := make([]engines.SearchResult, 0)
		for _, result := range output.Results {
			if result.QueryAlignment.SequenceId == seq.Id {
				seqResults = append(seqResults, result)
			}
		}
		service.Cache.Store(request.Namespace, seq, request.Params, seqResults)
	}

	// Concatenate the previously cached sequences to our results.
	for _, results := range cachedResults {
		output.Results = append(output.Results, results...)
	}

	// Return our results to the caller.
	offset := request.Offset
	N := request.Limit
	if N == -1 {
		N = len(output.Results)
	} else {
		N = min(offset+N, len(output.Results))
	}
	jsonData, _ := service.jsonFromSearchResults(request, output.Results[offset:N],
		false)
	writeJson(w, jsonData)
}

// Handler method for queueing a batch gene homology search.
func (service *geneHomologySearch) queueJob(w http.ResponseWriter,
	r *http.Request) {
	request, err := getSearchRequest(r)
	if err != nil {
		writeError(w, err.Error(), 400)
		return
	}

	// Validate the namespace.
	namespaceFound := false
	for _, ns := range service.Namespaces {
		if ns.Id == request.Namespace {
			namespaceFound = true
			break
		}
	}
	if !namespaceFound {
		errStr := fmt.Sprintf("Namespace %s not found", request.Namespace)
		writeError(w, errStr, 404)
		return
	}

	// Queue this request as a batch job.
	jobId, err := service.Batch.QueueJob(request)
	if err != nil {
		writeError(w, err.Error(), 500)
		return
	}

	log.Printf("Created batch job %s.", jobId.String())
	status := JobStatus{
		Id:        jobId,
		Namespace: request.Namespace,
		Status:    "queued"}
	jsonData, _ := json.Marshal(status)
	writeJson(w, jsonData)
}

// Handler method for retrieval of all batch statuses.
func (service *geneHomologySearch) getJobStatuses(w http.ResponseWriter,
	r *http.Request) {
	var statuses map[uuid.UUID]JobStatus

	// We filter our results by requested job IDs.
	jobIdsStr := r.FormValue("jobIds")
	jobIdStrs := strings.Split(jobIdsStr, ",")
	jobIds := make([]uuid.UUID, len(jobIdStrs))
	for i, jobIdStr := range jobIdStrs {
		jobId, err := uuid.Parse(jobIdStr)
		if err != nil {
			writeError(w, fmt.Sprintf("Invalid job ID: %s", jobIdStr), 400)
			return
		}
		jobIds[i] = jobId
	}

	// We can also filter by requested job status.
	statusFilter := r.FormValue("status")
	if statusFilter != "" && statusFilter != "queued" &&
		statusFilter != "processing" && statusFilter != "completed" {
		writeError(w, fmt.Sprintf("Invalid status filter: %s", statusFilter), 400)
		return
	}

	vars := mux.Vars(r)
	if namespace, ok := vars["ns"]; ok { // fetch statuses within namespace
		index := service.findNamespace(namespace)
		if index >= 0 {
			statuses = service.Batch.FetchJobStatuses(jobIds, namespace, statusFilter)
		} else {
			writeError(w, fmt.Sprintf("Namespace %s not found", namespace), 404)
			return
		}
	} else { // fetch all requested statuses.
		statuses = service.Batch.FetchJobStatuses(jobIds, "", statusFilter)
	}

	// Filter out the statuses that weren't requested.

	jsonData, _ := json.Marshal(statuses)
	writeJson(w, jsonData)
}

// Handler method for retrieval of a specific batch status.
func (service *geneHomologySearch) getJobStatus(w http.ResponseWriter,
	r *http.Request) {
	vars := mux.Vars(r)
	jobId, err := uuid.Parse(vars["job_id"])
	if err != nil {
		errStr := fmt.Sprintf("Invalid Job ID: %s", vars["job_id"])
		writeError(w, errStr, 400)
		return
	}
	log.Printf("Fetching status for job %s...", jobId.String())
	status, err := service.Batch.FetchJobStatus(jobId)
	if err != nil { // Job was not found
		writeError(w, err.Error(), 404)
	} else {
		jsonData, _ := json.Marshal(status)
		writeJson(w, jsonData)
	}
}

// Helper for extracting pagination parameters.
func extractPaginationParams(r *http.Request) (int, int, error) {
	v := r.URL.Query()
	offset := 0
	offsetVal := v.Get("offset")
	if offsetVal != "" {
		var err error
		offset, err = strconv.Atoi(offsetVal)
		if err != nil {
			err = fmt.Errorf("Error: Invalid results offset: %s", offsetVal)
			return -1, -1, err
		} else if offset < 0 {
			err = fmt.Errorf("Error: Invalid results offset: %d", offset)
			return -1, -1, err
		}
	}
	N := -1
	NVal := v.Get("limit")
	if NVal != "" {
		var err error
		N, err = strconv.Atoi(NVal)
		if err != nil {
			err = fmt.Errorf("Invalid results limit: %s", NVal)
			return -1, -1, err
		} else if N < 0 {
			err = fmt.Errorf("Invalid results limit: %d", N)
			return -1, -1, err
		}
	}
	return offset, N, nil
}

// Handler method for retrieval of batch results for a given job.
func (service *geneHomologySearch) getJob(w http.ResponseWriter,
	r *http.Request) {
	// Extract the job ID from the request.
	vars := mux.Vars(r)
	job, err := uuid.Parse(vars["job_id"])
	if err != nil {
		errStr := fmt.Sprintf("Invalid job ID: %s", vars["job_id"])
		writeError(w, errStr, 400)
		return
	}

	// Are we asked to return the query sequences for the job?
	includeQuerySeqs := false
	v := r.URL.Query()
	query_seq := v.Get("query_seq")
	if query_seq != "" {
		includeQuerySeqs, err = strconv.ParseBool(query_seq)
		if err != nil {
			writeError(w, err.Error(), 400)
			return
		}
	}

	// Are we asked to return a subset of our results?
	offset, N, err := extractPaginationParams(r)
	if err != nil {
		writeError(w, err.Error(), 400)
		return
	}

	// Fetch the results for the job.
	request, results, err := service.Batch.Fetch(job)
	statusCode := 500
	if err != nil {
		if strings.Contains(err.Error(), "not completed") {
			statusCode = 202
		} else if strings.Contains(err.Error(), "not found") {
			statusCode = 404
		}
		writeError(w, err.Error(), statusCode)
		return
	}

	// Apply pagination to the results.
	if offset > 0 {
		if N > 0 {
			results = results[offset:N]
		} else {
			results = results[offset:]
		}
	} else {
		if N > 0 {
			results = results[:N]
		}
	}

	// Wrap up the results.
	data, err := service.jsonFromSearchResults(request, results, includeQuerySeqs)
	if err != nil {
		writeError(w, err.Error(), 500)
		return
	}

	// Send the data along.
	writeJson(w, data)
}

// Constructs a gene homology search service that uses the given list
// of namespaces
func NewGeneHomologySearch() (SearchService, error) {
	service := new(geneHomologySearch)
	service.Name = "Gene Homology Search"
	service.Version = core.Version
	service.Port = -1
	var err error

	// Copy the namespaces into place.
	for _, namespace := range config.Namespaces {
		service.Namespaces = append(service.Namespaces, namespace)
	}

	// Find the databases for these namespaces on the local filesystem and
	// set up their associated engines.
	service.Databases = make([]core.GenomeDatabase, len(config.Databases))
	service.Engines = make(map[string]engines.SearchEngine)
	for i, namespace := range service.Namespaces {
		db := config.Databases[namespace.Database]
		service.Databases[i] = db
		engine, engine_present := service.Engines[db.Engine]
		if !engine_present {
			engine, err = engines.CreateSearchEngine(db.Engine)
			if err == nil {
				service.Engines[db.Engine] = engine
				// Validate the database with this engine to make sure it's been
				// properly prepared.
				err = engine.Validate(db)
			}
			if err != nil {
				return service, err
			}
		}
	}

	// Set up interactive caching.
	interactiveProcs := config.Service.MaxProcesses - config.Batch.MaxProcesses
	if interactiveProcs > 0 {
		service.Cache, _ = CreateSearchResultCache()
	}

	// The worker pool is set when it's first used.
	service.InteractivePool = nil

	// Set up diagnostics.
	service.Diags = NewSearchDiagnosticsManager()

	// Set up the batch system and provide a processing function.
	if config.Batch.MaxProcesses > 0 {
		log.Printf("Initializing batch system (%d workers)...",
			config.Batch.MaxProcesses)
		var bErr error
		service.Batch, bErr = NewBatchSystem(config.Batch,
			func(request SearchRequest) ([]engines.SearchResult, error) {

				// Find the database for the namespace.
				index := 0
				for index < len(service.Namespaces) {
					if service.Namespaces[index].Id == request.Namespace {
						break
					}
					index++
				}
				if index == len(service.Namespaces) {
					return nil, fmt.Errorf("Batch error: invalid request namespace: %s", request.Namespace)
				}
				db := service.Databases[index]

				// Retrieve the engine to use for the search.
				engine := service.Engines[db.Engine]

				// Is the database okay?
				err := engine.Validate(db)
				if err != nil {
					return nil, err
				}

				log.Printf("Searching namespace %s for alignments in %d sequences...",
					request.Namespace, len(request.Sequences))

				// Create a diagnostics entry for this request.
				diags := SearchServiceDiagRecord{
					Namespace:    request.Namespace,
					NumSequences: len(request.Sequences),
					Interactive:  false,
					Completed:    false,
				}
				diagIndex := service.Diags.Record(diags)

				// Do the alignment search.
				t1 := time.Now()
				results, err := engine.Search(db, request.Sequences, request.Params)
				t2 := time.Now()
				if err != nil {
					return nil, err
				}

				// Update the diagnostic record for this query.
				var size int
				for _, result := range results {
					size += int(unsafe.Sizeof(result))
				}
				service.Diags.Update(diagIndex, t2.Sub(t1), size)

				return results, err
			})
		if bErr != nil {
			return nil, bErr
		}
	}

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
	if config.Batch.Storage != "" && config.Batch.MaxProcesses > 0 {
		log.Print("Batch job processing enabled.")
		log.Printf("Batch results stored in %s and kept for %s.",
			config.Batch.Storage, config.Batch.DeleteAfter)
		api_v1.HandleFunc("/namespaces/{ns}/jobs", service.queueJob).Methods("POST")
		api_v1.HandleFunc("/namespaces/{ns}/jobs", service.getJobStatuses).Methods("GET")
		api_v1.HandleFunc("/jobs", service.getJobStatuses).Methods("GET")
		api_v1.HandleFunc("/jobs/{job_id}", service.getJob).Methods("GET")
		api_v1.HandleFunc("/jobs/{job_id}/status", service.getJobStatus).Methods("GET")
	} else {
		if config.Batch.Storage == "" {
			log.Print("No batch storage given. Batch processing disabled.")
		} else if config.Batch.MaxProcesses == 0 {
			log.Print("Batch.maxProcesses set to 0. Batch processing disabled.")
		}
	}
	service.Router = r

	return service, err
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
