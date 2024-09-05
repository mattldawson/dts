// Copyright (c) 2023 The KBase Project and its Contributors
// Copyright (c) 2023 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package nmdc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

const (
	nmdcBaseURL     = "https://api.microbiomedata.org"
	filePathPrefix = "/data/" // path exposing NMDC files available via Globus
)

// this error type is returned when a file is requested for which the requester
// does not have permission
type PermissionDeniedError struct {
	fileId string
}

func (e PermissionDeniedError) Error() string {
	return fmt.Sprintf("Can't access file %s: permission denied.", e.fileId)
}

// this error type is returned when a file is requested and is not found
type FileIdNotFoundError struct {
	fileId string
}

func (e FileIdNotFoundError) Error() string {
	return fmt.Sprintf("Can't access file %s: not found.", e.fileId)
}

// a mapping from file suffixes to format labels
var suffixToFormat = map[string]string{
	"bam":      "bam",
	"bam.bai":  "bai",
	"blasttab": "blast",
	"bz":       "bzip",
	"bz2":      "bzip2",
	"csv":      "csv",
	"faa":      "fasta",
	"fasta":    "fasta",
	"fasta.gz": "fasta",
	"fastq":    "fastq",
	"fastq.gz": "fastq",
	"fna":      "fasta",
	"gff":      "gff",
	"gff3":     "gff3",
	"gz":       "gz",
	"html":     "html",
	"info":     "texinfo",
	"out":      "text",
	"pdf":      "pdf",
	"tar":      "tar",
	"tar.gz":   "tar",
	"tar.bz":   "tar",
	"tar.bz2":  "tar",
	"tsv":      "tsv",
	"txt":      "text",
}

// this gets populated automatically with the keys in suffixToFormat
var supportedSuffixes []string

// a mapping from file format labels to mime types
var formatToMimeType = map[string]string{
	"bam":   "application/octet-stream",
	"bai":   "application/octet-stream",
	"csv":   "text/csv",
	"fasta": "text/plain",
	"fastq": "text/plain",
	"gff":   "text/plain",
	"gff3":  "text/plain",
	"gz":    "application/gzip",
	"bz":    "application/x-bzip",
	"bz2":   "application/x-bzip2",
	"tar":   "application/x-tar",
	"text":  "text/plain",
}

// a mapping from the JDP's reported file types to mime types
// (backup method for determining mime types)
var fileTypeToMimeType = map[string]string{
	"text":     "text/plain",
	"fasta":    "text/plain",
	"fasta.gz": "application/gzip",
	"fastq":    "text/plain",
	"fastq.gz": "application/gzip",
	"tab":      "text/plain",
	"tar.gz":   "application/x-tar",
	"tar.bz":   "application/x-tar",
	"tar.bz2":  "application/x-tar",
}

// attributes (slots) associated with NDMC data types
// (see https://microbiomedata.github.io/nmdc-schema/)
var nmdcDataAttributes = []string {
  // this list is giant--hopefully there's a way to programmatically
  // query NMDC for this
}

// extracts the file format from the name and type of the file
func formatFromFileName(fileName string) string {
	// make a list of the supported suffixes if we haven't yet
	if supportedSuffixes == nil {
		supportedSuffixes = make([]string, 0)
		for suffix := range suffixToFormat {
			supportedSuffixes = append(supportedSuffixes, suffix)
		}
	}

	// determine whether the file matches any of the supported suffixes,
	// selecting the longest matching suffix
	format := "unknown"
	longestSuffix := 0
	for _, suffix := range supportedSuffixes {
		if strings.HasSuffix(fileName, suffix) && len(suffix) > longestSuffix {
			format = suffixToFormat[suffix]
			longestSuffix = len(suffix)
		}
	}
	return format
}

// extracts the file format from the name and type of the file
func mimeTypeFromFormatAndTypes(format string, fileTypes []string) string {
	// try to match the file type to a mime type
	for _, fileType := range fileTypes {
		if mimeType, ok := fileTypeToMimeType[fileType]; ok {
			return mimeType
		}
	}
	// check the file format to see whether it matches a mime type
	if mimeType, ok := formatToMimeType[format]; ok {
		return mimeType
	}
	return ""
}

// extracts file type information from the given File
func fileTypesFromFile(file File) []string {
	// TODO: See https://pkg.go.dev/encoding/json?utm_source=godoc#example-RawMessage-Unmarshal
	// TODO: for an example of how to unmarshal a variant type.
	return []string{}
}

// extracts source information from the given metadata
func sourcesFromMetadata(md Metadata) []frictionless.DataSource {
	sources := make([]frictionless.DataSource, 0)
	piInfo := md.Proposal.PI
	if len(piInfo.LastName) > 0 {
		var title string
		if len(piInfo.FirstName) > 0 {
			title = fmt.Sprintf("%s, %s", piInfo.LastName, piInfo.FirstName)
			if len(piInfo.MiddleName) > 0 {
				title += fmt.Sprintf(" %s", piInfo.MiddleName)
			}
			if len(piInfo.Institution) > 0 {
				if len(piInfo.Country) > 0 {
					title += fmt.Sprintf(" (%s, %s)", piInfo.Institution, piInfo.Country)
				} else {
					title += fmt.Sprintf(" (%s)", piInfo.Institution)
				}
			} else if len(piInfo.Country) > 0 {
				title += fmt.Sprintf(" (%s)", piInfo.Country)
			}
		}
		var doiURL string
		if len(md.Proposal.AwardDOI) > 0 {
			doiURL = fmt.Sprintf("https://doi.org/%s", md.Proposal.AwardDOI)
		}
		source := frictionless.DataSource{
			Title: title,
			Path:  doiURL,
			Email: piInfo.EmailAddress,
		}
		sources = append(sources, source)
	}
	return sources
}

// creates a Frictionless DataResource-savvy name for a file:
// * the name consists of lower case characters plus '.', '-', and '_'
// * all forbidden characters encountered in the filename are removed
// * a number suffix is added if needed to make the name unique
func dataResourceName(filename string) string {
	name := strings.ToLower(filename)

	// remove any file suffix
	lastDot := strings.LastIndex(name, ".")
	if lastDot != -1 {
		name = name[:lastDot]
	}

	// replace sequences of invalid characters with '_'
	for {
		isInvalid := func(c rune) bool {
			return !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' && c != '-' && c != '.'
		}
		start := strings.IndexFunc(name, isInvalid)
		if start >= 0 {
			nameRunes := []rune(name)
			end := start + 1
			for end < len(name) && isInvalid(nameRunes[end]) {
				end++
			}
			if end < len(name) {
				name = name[:start] + string('_') + name[end+1:]
			} else {
				name = name[:start] + string('_')
			}
		} else {
			break
		}
	}

	return name
}

// file database appropriate for handling searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// database identifier
	Id string
	// ORCID identifier for database proxy
	Orcid string
	// HTTP client that caches queries
	Client http.Client
	// shared secret used for authentication
	Secret string
}

func NewDatabase(orcid string) (databases.Database, error) {
	if orcid == "" {
		return nil, fmt.Errorf("No ORCID ID was given")
	}

	// make sure we have a shared secret or an SSO token
	secret, haveSecret := os.LookupEnv("DTS_NMDC_SECRET")
	if !haveSecret {
		return nil, fmt.Errorf("No shared secret was found for NMDC authentication")
	}

	return &Database{
		Id:         "nmdc",
		Orcid:      orcid,
		Secret:     secret,
	}, nil
}

// adds an appropriate authorization header to given HTTP request
func (db Database) addAuthHeader(request *http.Request) {
	request.Header.Add("Authorization", fmt.Sprintf("Token %s_%s", db.Orcid, db.Secret))
}

// performs a GET request on the given resource, returning the resulting
// response and error
func (db *Database) get(resource string, values url.Values) (*http.Response, error) {
	var u *url.URL
	u, err := url.ParseRequestURI(jdpBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = resource
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("GET: %s", res))
	req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
	if err != nil {
		return nil, err
	}
	db.addAuthHeader(req)
	return db.Client.Do(req)
}

// performs a POST request on the given resource, returning the resulting
// response and error
func (db *Database) post(resource string, body io.Reader) (*http.Response, error) {
	u, err := url.ParseRequestURI(jdpBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = resource
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("POST: %s", res))
	req, err := http.NewRequest(http.MethodPost, res, body)
	if err != nil {
		return nil, err
	}
	db.addAuthHeader(req)
	req.Header.Set("Content-Type", "application/json")
	return db.Client.Do(req)
}

// data object type for JSON marshalling
// (see https://microbiomedata.github.io/nmdc-schema/DataObject/)
type DataObject struct {
  FileSizeBytes int `json:"file_size_bytes"`
  Md5Checksum string `json:"md5_checksum"`
  DataObjectType string `json:"data_object_type"`
  CompressionType string `json:"compression_type"`
  // NOTE: no representation of was_generated_by (abstract type) at the moment
  URL string `json:"url"`
  Type string `json:"type"`
  Id string `json:"id"`
  Name string `json:"name"`
  Description string `json:"description"`
  AlternativeIdentifiers []string `json:"alternative_identifiers,omitempty"`
}

func dataResourceFromDataObject(dataObject DataObject) frictionless.DataResource {
	return frictionless.DataResource{
		Id:        id,
		Name:      dataResourceName(file.Name),
		Path:      filePath,
		Format:    format,
		MediaType: mimeTypeFromFormatAndTypes(format, fileTypes),
		Bytes:     file.Size,
		Hash:      file.MD5Sum,
		Sources:   sources,
		Credit: credit.CreditMetadata{
			Identifier:   id,
			ResourceType: "dataset",
			Titles: []credit.Title{
				{
					Title: filePath,
				},
			},
			Dates: []credit.EventDate{
				{
					Date:  file.Date,
					Event: "Created",
				},
				{
					Date:  file.AddedDate,
					Event: "Accepted",
				},
				{
					Date:  file.ModifiedDate,
					Event: "Updated",
				},
			},
			Publisher: credit.Organization{
				OrganizationId:   "ROR:04xm1d337",
				OrganizationName: "Joint Genome Institute",
			},
			RelatedIdentifiers: []credit.PermanentID{
				{
					Id:               file.Metadata.Proposal.DOI,
					Description:      "Proposal DOI",
					RelationshipType: "IsCitedBy",
				},
				{
					Id:               file.Metadata.Proposal.AwardDOI,
					Description:      "Awarded proposal DOI",
					RelationshipType: "IsCitedBy",
				},
			},
			Contributors: []credit.Contributor{
				{
					ContributorType: "Person",
					// ContributorId: nothing yet
					Name:       strings.TrimSpace(fmt.Sprintf("%s, %s %s", pi.LastName, pi.FirstName, pi.MiddleName)),
					GivenName:  strings.TrimSpace(fmt.Sprintf("%s %s", pi.FirstName, pi.MiddleName)),
					FamilyName: strings.TrimSpace(pi.LastName),
					Affiliations: []credit.Organization{
						{
							OrganizationName: pi.Institution,
						},
					},
					ContributorRoles: "PI",
				},
			},
			Version: file.Date,
		},
	}
}

// fetches metadata for data objects based on the given parameters
func (db *Database) dataObjects(params url.Values) (databases.SearchResults, error) {
	var results databases.SearchResults

	idEncountered := make(map[string]bool) // keep track of duplicates

	// extract any requested "extra" metadata fields (and scrub them from params)
	var extraFields []string
	if params.Has("extra") {
		extraFields = strings.Split(params.Get("extra"), ",")
		params.Del("extra")
	}

	resp, err := db.get("data_objects/", params)
	if err != nil {
		return results, err
	}
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return results, err
	}
	type DataObjectResults struct {
    // NOTE: we only extract the results field for now
    Results []DataObject `json:"results"`
	}
	var dataObjectResults DataObjectResults
	err = json.Unmarshal(body, &dataObjectResults)
	if err != nil {
		return results, err
	}
  results.resources = dataResourcesFromDataObjects(dataObjectResults.Results)
	return results, nil
}

// fetches metadata for data objects associated with the given study
func (db *Database) dataObjectsForStudy(studyId string, pageNumber, pageSize int) (databases.SearchResults, error) {
	var results databases.SearchResults

	// extra any requested "extra" metadata fields (and scrub them from params)
	var extraFields []string
	if params.Has("extra") {
		extraFields = strings.Split(params.Get("extra"), ",")
		params.Del("extra")
	}

	resp, err := db.get(fmt.Sprintf("data_objects/study/%s", studyId), params)
	if err != nil {
		return results, err
	}
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return results, err
	}
	type DataObjectResults struct {
    // NOTE: we only extract the results field for now
    Results []DataObject `json:"results"`
	}
	var dataObjectResults DataObjectResults
	err = json.Unmarshal(body, &dataObjectResults)
	if err != nil {
		return results, err
	}
  results.resources = dataResourcesFromDataObjects(dataObjectResults.Results)
	return results, nil
}

// returns the page number and page size corresponding to the given Pagination
// parameters
func pageNumberAndSize(offset, maxNum int) (int, int) {
	pageNumber := 1
	pageSize := 100
	if offset > 0 {
		if maxNum == -1 {
			pageSize = offset
			pageNumber = 2
		} else {
			pageSize = maxNum
			pageNumber = offset/pageSize + 1
		}
	} else {
		if maxNum > 0 {
			pageSize = maxNum
		}
	}
	return pageNumber, pageSize
}

func (db Database) SpecificSearchParameters() map[string]interface{} {
  // for details about NMDC-specific search parameters, see
  // https://api.microbiomedata.org/docs#/find:~:text=Find%20NMDC-,metadata,-entities.
	return map[string]interface{}{
    "activity_id": "",
    "data_object_id": "",
    "fields": []string{},
    "filter": "",
    "sort": "",
		"sample_id": "",
		"study_id": "",
		"extra": []string{},
	}
}

// checks NMDC-specific search parameters
func (db Database) addSpecificSearchParameters(params map[string]json.RawMessage, p *url.Values) error {
	paramSpec := db.SpecificSearchParameters()
	for name, jsonValue := range params {
		switch name {
      case "activity_id", "data_object_id",
  p.Add(name, value)
	return nil
}

func (db *Database) Search(params databases.SearchParameters) (databases.SearchResults, error) {
  // fetch pagination parameters
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)

	p := url.Values{}

  // NMDC's "search" parameter is not yet implemented, so we ignore it for now
  // in favor of "filter"
	//p.Add("search", params.Query)
	if params.Status == databases.SearchFileStatusStaged {
		p.Add(`ff[file_status]`, "RESTORED")
	} else if params.Status == databases.SearchFileStatusUnstaged {
		p.Add(`ff[file_status]`, "PURGED")
	}
	p.Add("page", strconv.Itoa(pageNumber))
	p.Add("per_page", strconv.Itoa(pageSize))

  // decide which endpoint to call based on NMDC-specific parameters
	if params.Specific != nil {
    params.Specific
		err := db.addSpecificSearchParameters(params.Specific, &p)
		if err != nil {
			return databases.SearchResults{}, err
		}
	} else {
    // simply call the data_objects/ endpoint with the given query string
    p.Add("filter", params.Query) // FIXME: 
    return db.dataObjects(p)
  }

	return db.dataObjectsFromSearch(p)
}

func (db *Database) Resources(fileIds []string) ([]frictionless.DataResource, error) {
	// strip the "JDP:" prefix from our files and create a mapping from IDs to
	// their original order so we can hand back metadata accordingly
	strippedFileIds := make([]string, len(fileIds))
	indexForId := make(map[string]int)
	for i, fileId := range fileIds {
		strippedFileIds[i] = strings.TrimPrefix(fileId, "JDP:")
		indexForId[strippedFileIds[i]] = i
	}

	type MetadataRequest struct {
		Ids                []string `json:"ids"`
		Aggregations       bool     `json:"aggregations"`
		IncludePrivateData bool     `json:"include_private_data"`
	}
	data, err := json.Marshal(MetadataRequest{
		Ids:                strippedFileIds,
		Aggregations:       false,
		IncludePrivateData: true,
	})
	if err != nil {
		return nil, err
	}

	resp, err := db.post("search/by_file_ids/", bytes.NewReader(data))
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	type MetadataResponse struct {
		Hits struct {
			Hits []struct {
				Type   string `json:"_type"`
				Id     string `json:"_id"`
				Source struct {
					Date         string `json:"file_date"`
					AddedDate    string `json:"added_date"`
					ModifiedDate string `json:"modified_date"`
					FilePath     string `json:"file_path"`
					FileName     string `json:"file_name"`
					FileSize     int    `json:"file_size"`
					MD5Sum       string `json:"md5sum"`
					Metadata     Metadata
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	var jdpResp MetadataResponse
	err = json.Unmarshal(body, &jdpResp)
	if err != nil {
		return nil, err
	}

	// translate the response
	resources := make([]frictionless.DataResource, len(strippedFileIds))
	for i, md := range jdpResp.Hits.Hits {
		if md.Id == "" { // permissions problem
			return nil, &PermissionDeniedError{fileIds[i]}
		}
		index, found := indexForId[md.Id]
		if !found {
			return nil, &FileIdNotFoundError{fileIds[i]}
		}
		file := File{
			Id:           md.Id,
			Name:         md.Source.FileName,
			Path:         md.Source.FilePath,
			Date:         md.Source.Date,
			AddedDate:    md.Source.AddedDate,
			ModifiedDate: md.Source.ModifiedDate,
			Size:         md.Source.FileSize,
			Metadata:     md.Source.Metadata,
			MD5Sum:       md.Source.MD5Sum,
		}
		resources[index] = dataResourceFromFile(file)
		if resources[index].Path == "" || resources[index].Path == "/" { // permissions probem
			return nil, &PermissionDeniedError{fileIds[index]}
		}

		// fill in holes where we can and patch up discrepancies
		// NOTE: we don't retrieve hits.hits._source.file_type because it can be
		// NOTE: either a string or an array of strings, and I'm just trying for a
		// NOTE: solution
		resources[index].Format = formatFromFileName(resources[index].Path)
		resources[index].MediaType = mimeTypeFromFormatAndTypes(resources[index].Format, []string{})
	}
	return resources, err
}

func (db *Database) StageFiles(fileIds []string) (uuid.UUID, error) {
  // NMDC keeps all of its NERSC data on disk, so all files are already staged.
  // We simply generate a new UUID that can be handed to db.StagingStatus,
  // which returns databases.StagingStatusSucceeded.
  //
  // "We may eventually use tape but don't need to yet." -Shreyas Cholia, 2024-09-04
  return uuid.New(), nil
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
  // all files are hot!
	return databases.StagingStatusSucceeded, nil
}

func (db *Database) Endpoint() (endpoints.Endpoint, error) {
	return endpoints.NewEndpoint(config.Databases[db.Id].Endpoint)
}

func (db *Database) LocalUser(orcid string) (string, error) {
	// no current mechanism for this
	return "localuser", nil
}
