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

// This type represents a single file entry in a JDP ElasticSearch result.
type jdpFile struct {
	// unique ID used by the DTS to manipulate the file
	Id string `json:"_id"`
	// name of the file (excluding Path)
	Name string `json:"file_name"`
	// directory in which the file sits
	Path string `json:"file_path"`
	// file size (bytes)
	Size int `json:"file_size"`
	// file metadata
	Metadata jdpMetadata `json:"metadata"`
	// name of the user that owns the file
	Owner string `json:"file_owner"`
	// date that the file was added
	AddedDate string `json:"added_date"`
	// date of last modification to the file
	ModifiedDate string `json:"modified_date"`
	// date file will be purged
	PurgeDate string `json:"dt_to_purge"`
	// file origination date? (FIXME: is this ever different from AddedDate??)
	Date string `json:"file_date"`
	// integer ID representing the status of the file
	StatusId int `json:"file_status_id"`
	// string describing the status of the file
	Status string `json:"file_status"`
	// list of types corresponding to this file
	Types []string `json:"file_type"`
	// MD5 checksum
	MD5Sum string `json:"md5sum"`
	// user with access to the file (FIXME: not the owner??)
	User string `json:"user"`
	// name of UNIX group with access to the file
	Group string `json:"file_group"`
	// UNIX file permissions (a string containing the octal representation)
	Permissions string `json:"file_permissions"`
	// name of the group that produced the file's data
	DataGroup string `json:"data_group"`
	// portal detail ID (type??)
	PortalDetailId string `json:"portal_detail_id"`
}

// this type represents metadata associated with a JDPFile
type jdpMetadata struct {
	// proposal info
	Proposal struct {
		// DOI of the awarded proposal
		AwardDOI string `json:"award_doi"`
		// info about the Principal Investigator
		PI struct {
			// PI's last name
			LastName string `json:"last_name"`
			// PI's first name
			FirstName string `json:"first_name"`
			// PI's middle name (if any)
			MiddleName string `json:"middle_name"`
			// PI's email address
			EmailAddress string `json:"email_address"`
			// name of academic or industrial institution
			Institution string `json:"institution"`
			// country of institution/PI
			Country string `json:"country"`
		} `json:"pi"`
		// date of proposal approval
		DateApproved string `json:"date_approved"`
		// proposal DOI (how is this different from AwardDOI?)
		DOI string `json:"doi"`
	} `json:"proposal"`
	// status indicating whether data is "Restricted" or "Unrestricted"
	DataUtilizationЅtatus string `json:"data_utilization_status"`
	// GOLD-related metadata
	GoldData struct {
		// stamp ID
		StampId string `json:"gold_stamp_id"`
		// project URL
		ProjectURL string `json:"gold_project_url"`
		// display name
		DisplayName string `json:"display_name"`
	} `json:"gold_data"`
	// sequencing project metadata
	SequencingProject struct {
		// name of scientific program to which project belongs
		ScientificProgramName string `json:"scientific_program_name"`
	} `json:"sequencing_project"`
	// sequencing project identifier
	SequencingProjectId int `json:"sequencing_project_id"`
	// NCBI taxon metadata
	NCBITaxon struct {
		Order   string `json:"ncbi_taxon_order"`
		Family  string `json:"ncbi_taxon_family"`
		Genus   string `json:"ncbi_taxon_genus"`
		Species string `json:"ncbi_taxon_species"`
	} `json:"ncbi_taxon"`
	// NCBI taxon identifier
	NCBITaxonId int `json:"ncbi_taxon_id"`
	// portal metadata
	Portal struct {
		DisplayLocation []string `json:"display_location"`
	} `json:"portal"`
	// final project delivery metadata
	FinalDeliveryProject struct {
		ProductSearchCategory string `json:"product_search_category"`
	} `json:"final_deliv_project"`
}

// creates a DataResource from a jdpFile (including metadata)
func dataResourceFromJdpFile(file jdpFile) DataResource {
	var format string
	if strings.Contains(file.Name, ".") {
		format = file.Name[:strings.Index(file.Name, ".")]
	}
	return DataResource{
		Name:   fmt.Sprintf("JDP:%s", file.Metadata.SequencingProjectId),
		Path:   filepath.Join(file.Path, file.Name),
		Format: format,
		Bytes:  file.Size,
		Hash:   file.MD5Sum,
		Credit: credit.CreditMetadata{
			Identifier: fmt.Sprintf("JDP:%s", file.Metadata.SequencingProjectId),
		},
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

	return &JdpDatabase{
		Id:         dbName,
		BaseURL:    dbConfig.URL,
		StagingIds: make(map[uuid.UUID]int),
	}, nil
}

// this helper extracts files for the JDP /search GET query with given parameters
func (db *JdpDatabase) filesForSearch(params url.Values) (SearchResults, error) {
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
				type JDPResult struct {
					Organisms []struct {
						Files []jdpFile `json:"files"`
					} `json:"organisms"`
				}
				var jdpResults JDPResult
				results.Resources = make([]DataResource, 0)
				err = json.Unmarshal(body, &jdpResults)
				if err == nil {
					for _, org := range jdpResults.Organisms {
						resources := make([]DataResource, len(org.Files))
						for i, file := range org.Files {
							resources[i] = dataResourceFromJdpFile(file)
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
