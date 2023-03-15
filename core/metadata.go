package core

// this type represents metadata associated with a file
type Metadata struct {
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
