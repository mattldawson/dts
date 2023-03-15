package core

// This type represents a single file entry in an ElasticSearch result.
type File struct {
	// integer ID assigned to the file
	Id int `json:"file_id"`
	// name of the file (excluding Path)
	Name string `json:"file_name"`
	// directory in which the file sits
	Path string `json:"file_path"`
	// file size (bytes)
	Size int `json:"file_size"`
	// file metadata
	Metadata Metadata `json:"metadata"`
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
