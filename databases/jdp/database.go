package jdp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"dts/config"
	"dts/core"
	"dts/credit"
)

// the directory in which all JDP files reside, which is the absolute path
// associated with all Frictionless Data Resource relative POSIX paths
var filePathPrefix = "/global/dna/dm_archive/"

// a mapping from file suffixes to format labels
var suffixToFormat = map[string]string{
	"bam":     "bam",
	"bam.bai": "bai",
	"csv":     "csv",
	"faa":     "fasta",
	"fasta":   "fasta",
	"fastq":   "fastq",
	"gff":     "gff",
	"gff3":    "gff3",
	"gz":      "gz",
	"bz":      "bzip",
	"bz2":     "bzip2",
	"tar":     "tar",
	"tar.gz":  "tar",
	"tar.bz":  "tar",
	"tar.bz2": "tar",
	"txt":     "text",
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
	"txt":   "text/plain",
}

// a mapping from the JDP's reported file types to mime types
// (backup method for determining mime types)
var fileTypeToMimeType = map[string]string{
	"text":     "text/plain",
	"fasta":    "text/plain",
	"fastq":    "text/plain",
	"fastq.gz": "application/gzip",
	"tab":      "text/plain",
	"tar.gz":   "application/x-tar",
	"tar.bz":   "application/x-tar",
	"tar.bz2":  "application/x-tar",
}

// extracts the file format from the name and type of the file
func formatFromFileName(fileName string) string {
	// make a list of the supported suffixes if we haven't yet
	if supportedSuffixes == nil {
		supportedSuffixes = make([]string, 0)
		for suffix, _ := range suffixToFormat {
			supportedSuffixes = append(supportedSuffixes, suffix)
		}
	}

	// determine whether the file matches any of the supported suffixes
	for _, suffix := range supportedSuffixes {
		if strings.HasSuffix(fileName, suffix) {
			return suffixToFormat[suffix]
		}
	}
	return ""
}

// extracts the file format from the name and type of the file
func mimeTypeFromFormatAndTypes(format string, fileTypes []string) string {
	// check the file format to see whether it matches a mime type
	if mimeType, ok := formatToMimeType[format]; ok {
		return mimeType
	} else {
		// try to match the file type to a mime type
		for _, fileType := range fileTypes {
			if mimeType, ok := fileTypeToMimeType[fileType]; ok {
				return mimeType
			}
		}
	}
	return ""
}

// extracts the ITS project ID from the given metadata field, or returns -1 if
// no such ID can be extracted
func itsProjectIdFromMetadata(md Metadata, field json.RawMessage) int {
	// is it an integer?
	var intId int
	err := json.Unmarshal(field, &intId)
	if err == nil {
		return intId
	} else { // nope!
		// how about an integer list?
		var listId []int
		err = json.Unmarshal(field, &listId)
		if err == nil {
			return listId[0] // use the first ID
		}
	}
	return -1
}

// extracts file type information from the given File
func fileTypesFromFile(file File) []string {
	// TODO: See https://pkg.go.dev/encoding/json?utm_source=godoc#example-RawMessage-Unmarshal
	// TODO: for an example of how to unmarshal a variant type.
	return []string{}
}

// extracts source information from the given metadata
func sourcesFromMetadata(md Metadata) []core.DataSource {
	sources := make([]core.DataSource, 0)
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
		source := core.DataSource{
			Title: title,
			Path:  doiURL,
			Email: piInfo.EmailAddress,
		}
		sources = append(sources, source)
	}
	return sources
}

// creates a credit metadata item from JDP file metadata
func creditFromMetadata(md Metadata, itsFieldName string) credit.CreditMetadata {
	// construct the CURIE identifier for credit metadata
	// NOTE: the ITS field name we are given doesn't always point to a valid
	// NOTE: identifier
	var itsField json.RawMessage
	if strings.Contains(itsFieldName, "analysis_project_id") {
		itsField = md.AnalysisProjectId
	} else if strings.Contains(itsFieldName, "sequencing_project_id") {
		itsField = md.SequencingProjectId
	}
	itsProjectId := itsProjectIdFromMetadata(md, itsField)

	var curieId string
	if itsProjectId != -1 {
		curieId = fmt.Sprintf("JDP:%d", itsProjectId)
	} else {
		curieId = "JDP:unknown"
	}

	crd := credit.CreditMetadata{
		Identifier:   curieId,
		ResourceType: "dataset",
	}

	crd.Dates = []credit.EventDate{
		credit.EventDate{
			Date:  md.Proposal.DateApproved,
			Event: "approval",
		},
	}
	pi := md.Proposal.PI
	crd.Contributors = []credit.Contributor{
		credit.Contributor{
			ContributorType: "Person",
			// ContributorId: nothing yet
			Name:       strings.TrimSpace(fmt.Sprintf("%s, %s %s", pi.LastName, pi.FirstName, pi.MiddleName)),
			CreditName: strings.TrimSpace(fmt.Sprintf("%s, %s %s", pi.LastName, pi.FirstName, pi.MiddleName)),
			Affiliations: []credit.Organization{
				credit.Organization{
					OrganizationName: pi.Institution,
				},
			},
			ContributorRoles: "PI",
		},
	}
	return crd
}

// creates a DataResource from a File (including metadata) and the name of the
// field from which to extract the ITS project ID
func dataResourceFromFile(file File, itsFieldName string) core.DataResource {
	format := formatFromFileName(file.Name)
	fileTypes := fileTypesFromFile(file)
	sources := sourcesFromMetadata(file.Metadata)

	// we use relative file paths in accordance with the Frictionless
	// Data Resource specification
	filePath := filepath.Join(strings.ReplaceAll(file.Path, filePathPrefix, ""), file.Name)

	return core.DataResource{
		Name:      file.MD5Sum,
		Path:      filePath,
		Format:    format,
		MediaType: mimeTypeFromFormatAndTypes(format, fileTypes),
		Bytes:     file.Size,
		Hash:      file.MD5Sum,
		Sources:   sources,
		Credit:    creditFromMetadata(file.Metadata, itsFieldName),
	}
}

// file database appropriate for handling JDP searches and transfers
// (implements the core.Database interface)
type Database struct {
	// database identifier
	Id string
	// JDP base URL
	BaseURL string
	// API token used for authentication
	ApiToken string
	// mapping from staging UUIDs to JDP restoration request ID
	StagingIds map[uuid.UUID]int
}

func NewDatabase(dbName string) (core.Database, error) {
	dbConfig, ok := config.Databases[dbName]
	if !ok {
		return nil, fmt.Errorf("Database %s not found", dbName)
	}

	// read our API access token from a file
	// FIXME

	return &Database{
		Id:         dbName,
		BaseURL:    dbConfig.URL,
		StagingIds: make(map[uuid.UUID]int),
	}, nil
}

// this helper extracts files for the JDP /search GET query with given parameters
func (db *Database) filesForSearch(params url.Values) (core.SearchResults, error) {
	var results core.SearchResults

	u, err := url.ParseRequestURI(db.BaseURL)
	if err == nil {
		u.Path = "search"
		u.RawQuery = params.Encode()

		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type JDPResults struct {
					Organisms []struct {
						Files []File `json:"files"`
						// this determines which file field is used for ITS project IDs
						GroupedBy string `json:"grouped_by"`
					} `json:"organisms"`
				}
				var jdpResults JDPResults
				results.Resources = make([]core.DataResource, 0)
				err = json.Unmarshal(body, &jdpResults)
				if err == nil {
					for _, org := range jdpResults.Organisms {
						resources := make([]core.DataResource, 0)
						for _, file := range org.Files {
							res := dataResourceFromFile(file, org.GroupedBy)
							if res.Format != "" {
								resources = append(resources, res)
							}
						}
						results.Resources = append(results.Resources, resources...)
					}
				}
			}
		}
	}
	return results, err
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

func (db *Database) Search(params core.SearchParameters) (core.SearchResults, error) {
	// we assume the JDP interface for ElasticSearch queries
	// (see https://files.jgi.doe.gov/apidoc/)
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)

	p := url.Values{}
	p.Add("q", params.Query)
	p.Add("p", strconv.Itoa(pageNumber))
	p.Add("x", strconv.Itoa(pageSize))

	return db.filesForSearch(p)
}

func (db *Database) FilesStaged(fileIds []string) (bool, error) {
	// fetch the paths for the files with the given IDs that are RESTORED
	type FileFilter struct {
		Ids      []string `json:"_id"`
		Statuses []string `json:"file_status"`
	}
	ff, err := json.Marshal(FileFilter{Ids: fileIds, Statuses: []string{"RESTORED"}})
	if err != nil {
		return false, err
	}
	p := url.Values{}
	p.Add("ff=", string(ff))
	results, err := db.filesForSearch(p)
	if err != nil {
		return false, err
	}

	// Did we get back all files we requested? If so, all files are staged.
	return len(results.Resources) == len(fileIds), nil
}

func (db *Database) StageFiles(fileIds []string) (uuid.UUID, error) {
	var xferId uuid.UUID

	// construct a POST request to restore archived files with the given IDs
	type RestoreRequest struct {
		Ids []string `json:"ids"`
	}
	data, err := json.Marshal(RestoreRequest{Ids: fileIds})
	if err != nil {
		return xferId, err
	}

	u, err := url.ParseRequestURI(db.BaseURL)
	if err == nil {
		u.Path = "request_archived_files"

		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Post(request, "application/json", bytes.NewReader(data))
		defer resp.Body.Close()
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type RestoreResponse struct {
					RequestId int `json:"request_id"`
				}

				var jdpResp RestoreResponse
				err = json.Unmarshal(body, &jdpResp)
				if err == nil {
					xferId = uuid.New()
					db.StagingIds[xferId] = jdpResp.RequestId
				}
			}
		}
	}

	return xferId, err
}

func (db *Database) StagingStatus(id uuid.UUID) (core.StagingStatus, error) {
	if restoreId, found := db.StagingIds[id]; found {
		u, err := url.ParseRequestURI(db.BaseURL)
		if err == nil {
			u.Path = fmt.Sprintf("request_archived_files/requests/%d", restoreId)

			request := fmt.Sprintf("%v", u)
			var resp *http.Response
			resp, err = http.Get(request)
			defer resp.Body.Close()
			if err == nil {
				var body []byte
				body, err = io.ReadAll(resp.Body)
				if err == nil {
					type JDPResult struct {
						Status string `json:"status"` // "ready" or not
					}
					var jdpResult JDPResult
					err = json.Unmarshal(body, &jdpResult)
					if err == nil {
						statusForString := map[string]core.StagingStatus{
							"ready": core.StagingStatusSucceeded,
						}
						if status, ok := statusForString[jdpResult.Status]; ok {
							return status, nil
						} else {
							return status, fmt.Errorf("Unrecognized staging status string: %s", jdpResult.Status)
						}
					}
				}
			}
		}
		return core.StagingStatusUnknown, err
	} else {
		return core.StagingStatusUnknown, nil
	}
}
