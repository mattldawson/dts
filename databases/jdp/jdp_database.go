package databases

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
	"dts/databases/credit"
)

const suffixToFormat = map[string]string{
	"csv":   "csv",
	"faa":   "fasta",
	"fasta": "fasta",
	"fastq": "fastq",
	"gff":   "gff",
	"gff3":  "gff",
	"gz":    "gz",
	"txt":   "text",
}

const suffixToMimeType = map[string]string{
	"csv":   "text/csv",
	"faa":   "text/plain",
	"fasta": "text/plain",
	"fastq": "text/plain",
	"gff":   "text/plain",
	"gff3":  "text/plain",
	"gz":    "application/gzip",
	"txt":   "text/plain",
}

const fileTypeToMimeType = map[string]string{
	"text":     "text/plain",
	"fasta":    "text/plain",
	"fastq":    "text/plain",
	"fastq.gz": "application/gzip",
	"tab":      "text/plain",
	"tar.gz":   "application/gzip",
}

// extracts the file format from the name and type of the file
func formatFromFileName(fileName string) string {
	suffix := filepath.Ext(fileName)
	if format, ok := suffixToFormat[suffix]; ok {
		return format
	} else {
		return ""
	}
}

// extracts the file format from the name and type of the file
func mimeTypeFromFileNameAndTypes(fileName string, fileTypes []string) string {
	suffix := filepath.Ext(fileName)
	// check the file suffix to see whether it matches a mime type
	if mimeType, ok := suffixToMimeType[suffix]; ok {
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

// extracts file type information from the given jdpFile
func fileTypesFromFile(file jdpFile) []string {
	// TODO: See https://pkg.go.dev/encoding/json?utm_source=godoc#example-RawMessage-Unmarshal
	// TODO: for an example of how to unmarshal a variant type.
	return []string{}
}

// extracts source information from the given jdpFile
func sourcesFromFile(file jdpFile) []DataSource {
	sources := make([]DataSource, 0)
	piInfo := file.Metadata.Proposal
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
		if len(file.Metadata.Proposal.AwardDOI) > 0 {
			doiURL = fmt.Sprintf("https://doi.org/%s", file.Metadata.Proposal.AwardDOI)
		}
		source := DataSource{
			Title: title,
			Path:  doiURL,
			Email: piInfo.EmailAddress,
		}
		sources = append(sources, source)
	}
	return sources
}

// extracts KBase credit engine information from the given jdpFile
func creditFromFile(file jdpFile) credit.CreditMetadata {
	var itsProjectId int

	creditData := credit.CreditMetadata{
		Identifier:   fmt.Sprintf("JDP:%s", itsProjectId),
		ResourceType: "dataset",
	}
	return creditData
}

// creates a DataResource from a jdpFile (including metadata)
func dataResourceFromFile(file jdpFile) DataResource {
	fileTypes := fileTypesFromFile(file)
	return DataResource{
		Name:      fmt.Sprintf("JDP:%s", file.Metadata.SequencingProjectId),
		Path:      filepath.Join(file.Path, file.Name),
		Format:    formatFromFileName(file.Name),
		MediaType: mimeTypeFromFileNameAndTypes(file.Name, fileTypes),
		Bytes:     file.Size,
		Hash:      file.MD5Sum,
		Sources:   sourcesFromFile(file),
		Credit:    creditFromFile(file),
	}
}

// file database appropriate for handling JDP searches and transfers
type JdpDatabase struct {
	// database identifier
	Id string
	// JDP base URL
	BaseURL string
	// API token used for authentication
	ApiToken string
	// mapping from staging UUIDs to JDP restoration request ID
	StagingIds map[uuid.UUID]int
}

func NewJdpDatabase(dbName string) (Database, error) {
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
func (db *Database) filesForSearch(params url.Values) (SearchResults, error) {
	var results SearchResults

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
						Files []jdpFile `json:"files"`
					} `json:"organisms"`
				}
				var jdpResults JDPResults
				results.Resources = make([]DataResource, 0)
				err = json.Unmarshal(body, &jdpResults)
				if err == nil {
					for _, org := range jdpResults.Organisms {
						resources := make([]DataResource, len(org.Files))
						for i, file := range org.Files {
							resources[i] = dataResourceFromFile(file)
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

func (db *JdpDatabase) Search(params SearchParameters) (SearchResults, error) {
	// we assume the JDP interface for ElasticSearch queries
	// (see https://files.jgi.doe.gov/apidoc/)
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)

	p := url.Values{}
	p.Add("q", params.Query)
	p.Add("p", strconv.Itoa(pageNumber))
	p.Add("x", strconv.Itoa(pageSize))

	return db.filesForSearch(p)
}

func (db *JdpDatabase) FilesStaged(fileIds []string) (bool, error) {
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

func (db *JdpDatabase) StageFiles(fileIds []string) (uuid.UUID, error) {
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

func (db *JdpDatabase) StagingStatus(id uuid.UUID) (StagingStatus, error) {
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
						statusForString := map[string]StagingStatus{
							"ready": StagingStatusSucceeded,
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
		return StagingStatusUnknown, err
	} else {
		return StagingStatusUnknown, nil
	}
}
