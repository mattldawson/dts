package credit_metadata

/*
 * Container for an instance of credit metadata; includes the credit metadata itself and metadata for the credit metadata.
 */
type CreditMetadataEntry struct {
	/*
	 * the credit metadata itself
	 */
	CreditMetadata CreditMetadata `json:"CreditMetadata"`
	/*
	 * the version of the credit metadata schema used
	 */
	CreditMetadataSchemaVersion string `json:"CreditMetadataSchemaVersion"`
	/*
	 * KBase workspace ID of the user who added this entry
	 */
	SavedBy string `json:"SavedBy"`
	/*
	 * unix timestamp for the addition of this set of credit metadata
	 */
	Timestamp int `json:"Timestamp"`
}

/*
  - Represents the credit metadata associated with a workspace object.

In the following documentation, 'Resource' is used to refer to the workspace object
that the CM pertains to.

The 'resource_type' field should be filled using values from the DataCite
resourceTypeGeneral field:

https://support.datacite.org/docs/datacite-metadata-schema-v44-mandatory-properties#10a-resourcetypegeneral

Currently the KBase workspace only supports credit metadata for objects of type
'dataset'; anything else will return an error.

The license may be supplied either as an URL pointing to licensing information for
the resource, or using a license name. See https://choosealicense.com/appendix/ for
a list of common open source licenses.

Required fields are:
- identifier
- versioning information: if the resource does not have an explicit version number,
one or more dates should be supplied: ideally the date of resource publication and
the last update (if applicable).
- contributors (one or more required)
- titles (one or more required)

The resource_type field is required, but as there is currently only a single valid
value, 'dataset', it is automatically populated if no value is supplied.
*/
type CreditMetadata struct {
	/*
	 * list of strings of freeform text providing extra information about this credit metadata.
	 */
	Comment string `json:"Comment"`
	/*
	 * A brief description or abstract for the resource being represented.
	 */
	Description string `json:"Description"`
	/*
	 * resolvable persistent unique identifier for the resource. Should be in the format <database name>:<identifier within database>.
	 */
	Identifier string `json:"Identifier"`
	/*
		 * usage license for the resource. May be a text string or an URL. Abbreviations should be spelled out where possible (e.g. 'Creative Commons 4.0' instead of 'CC-BY-4.0'). The license is interpreted as an URL and checked for well-formedness if it starts with a series of letters, a colon, and slashes, e.g. "http://"; "https://"; "ftp://".

	All data published at KBase is done so under a Creative Commons 0 or Creative Commons 4.0 license.

	*/
	License string `json:"License"`
	/*
	 * the broad type of the source data for this workspace object. 'dataset' is the only valid value currently.
	 */
	ResourceType string `json:"ResourceType"`
	/*
	 * the version of the resource. This must be an absolute version, not a relative version like 'latest'.
	 */
	Version string `json:"Version"`
	/*
	 * a list of people and/or organizations who contributed to the resource.
	 */
	Contributors []Contributor `json:"Contributors"`
	/*
	 * a list of relevant lifecycle events for the resource.
	 */
	Dates []EventDate `json:"Dates"`
	/*
	 * funding sources for the resource.
	 */
	Funding []FundingReference `json:"Funding"`
	/*
	 * other resolvable persistent unique IDs related to the resource.
	 */
	RelatedIdentifiers []PermanentID `json:"RelatedIdentifiers"`
	/*
	 * online repository for a dataset.
	 */
	Repository Organization `json:"Repository"`
	/*
	 * one or more titles for the resource. At least one title of title_type "title" must be provided.
	 */
	Titles []Title `json:"Titles"`
}

/*
  - Represents a contributor to the resource.

Contributors must have a 'contributor_type', either 'Person' or 'Organization', and
a 'name'.

The 'credit_name' field is used to store the name of a person as it would appear in
a citation. If there is no 'credit_name' supplied, the 'name' field would be used
in citations.
For example:

	name:         Hubert George Wells
	credit_name:  Wells, HG

	name:         Alexandria Ocasio-Cortez
	credit_name:  Ocasio-Cortez, A

	name:         Helena Bonham Carter
	credit_name:  Bonham Carter, H

The 'contributor_role' field takes values from the DataCite and CRediT contributor
roles vocabularies. For more information on these resources and choosing the
appropriate roles, please see the following links:

DataCite contributor roles: https://support.datacite.org/docs/datacite-metadata-schema-v44-recommended-and-optional-properties#7a-contributortype

CRediT contributor role taxonomy: https://credit.niso.org
*/
type Contributor struct {
	/*
	 * must be either 'Person' or 'Organization'
	 */
	ContributorType string `json:"ContributorType"`
	/*
	 * persistent unique identifier for the contributor; this might be an ORCID for an individual, or a ROR ID for an organization.
	 */
	ContributorId string `json:"ContributorId"`
	/*
	 * contributor name. For organizations, this should be the full (unabbreviated) name; for a person, the full name should be entered.
	 */
	Name string `json:"Name"`
	/*
	 * for a person, how the name should appear in a citation.
	 */
	CreditName string `json:"CreditName"`
	/*
	 * list of organizations with which the contributor is affiliated. For contributors that represent an organization, this may be a parent organization (e.g. KBase, US DOE; Arkin lab, LBNL).
	 */
	Affiliations []Organization `json:"Affiliations"`
	/*
	 * list of roles played by the contributor when working on the resource.
	 */
	ContributorRoles string `json:"ContributorRoles"`
}

/*
  - Represents an event in the lifecycle of a resource and the date it occurred on.

See https://support.datacite.org/docs/datacite-metadata-schema-v44-recommended-and-optional-properties#8-date for more information on the events.
*/
type EventDate struct {
	/*
	 * the date associated with the event. The date may be in the format YYYY, YYYY-MM, or YYYY-MM-DD.
	 */
	Date string `json:"Date"`
	/*
	 * the nature of the resource-related event that occurred on that date
	 */
	Event string `json:"Event"`
}

/*
  - Represents a funding source for a resource, including the funding body and the grant awarded.

The 'funder_name' field is required; all others are optional.

Recommended resources for organization identifiers include:
  - Research Organization Registry, http://ror.org
  - International Standard Name Identifier, https://isni.org
  - Crossref Funder Registry, https://www.crossref.org/services/funder-registry/

Some organizations may have a digital object identifier (DOI).
*/
type FundingReference struct {
	/*
	 * code for the grant, assigned by the funder
	 */
	GrantId string `json:"GrantId"`
	/*
	 * title for the grant
	 */
	GrantTitle string `json:"GrantTitle"`
	/*
	 * URL for the grant
	 */
	GrantUrl string `json:"GrantUrl"`
	/*
	 * the funder for the grant or award
	 */
	Funder Organization `json:"Funder"`
}

/*
  - Represents an organization.

Recommended resources for organization identifiers and canonical organization names include:
  - Research Organization Registry, http://ror.org
  - International Standard Name Identifier, https://isni.org
  - Crossref Funder Registry, https://www.crossref.org/services/funder-registry/

For example, the US DOE would be entered as:

	organization_name: United States Department of Energy
	organization_id:   ROR:01bj3aw27
*/
type Organization struct {
	/*
	 * persistent unique identifier for the organization in the format <database name>:<identifier within database>
	 */
	OrganizationId string `json:"OrganizationId"`
	/*
	 * common name of the organization; use the name recommended by ROR if possible.
	 */
	OrganizationName string `json:"OrganizationName"`
}

/*
  - Represents a persistent unique identifier for an entity, with an optional relationship to some other entity.

The 'id' field is required; all other fields are optional.

The values in the 'relationship_type' field come from controlled vocabularies maintained by DataCite and Crossref. See the documentation links below for more details.

DataCite relation types: https://support.datacite.org/docs/datacite-metadata-schema-v44-recommended-and-optional-properties#12b-relationtype

Crossref relation types: https://www.crossref.org/documentation/schema-library/markup-guide-metadata-segments/relationships/
*/
type PermanentID struct {
	/*
	 * persistent unique ID for an entity. Should be in the format <database name>:<identifier within database>.
	 */
	Id string `json:"Id"`
	/*
	 * description of that entity
	 */
	Description string `json:"Description"`
	/*
		 * The relationship between the ID and some other entity.
	For example, when a PermanentID class is used to represent objects in the CreditMetadata field 'related_identifiers', the 'relationship_type' field captures the relationship between the CreditMetadata and this ID.

	*/
	RelationshipType string `json:"RelationshipType"`
}

/*
  - Represents the title or name of a resource.

The 'title_string' field is required; if no value is supplied for 'title_type', it
defaults to 'title'.

If the title is in a language other than English, the 'title_type' should be set to
'translated_title', and the appropriate BCP-47 tag supplied in the 'title_language'
field.

Note that the workspace checks that the 'title_language' field adheres to IETF
BCP-47 syntax rules, but it does not check the validity of the tag.
*/
type Title struct {
	/*
	 * a string used as a title for a resource
	 */
	Title string `json:"Title"`
	/*
	 * a descriptor for the title. The default value is 'title'.
	 */
	TitleType string `json:"TitleType"`
	/*
	 * language that the title is in, as a IETF BCP-47 tag.
	 */
	TitleLanguage string `json:"TitleLanguage"`
}
