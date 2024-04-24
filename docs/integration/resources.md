# Provide Unique IDs and Metadata for Your Files

If you're reading this, you probably belong to an organization that maintains a
database containing many files of different sorts, and you probably want to make
it easier for researchers to access (and cite!) the data in these files.

In order for a file to be made available to users of BER's Data Transfer System
(DTS), it must have a unique identifier by which users can refer to it. The
file's name is probably not unique, so it's not an appropriate identifier.
Unique file identifiers are such a basic need that your organization probably
already has its own set.

Any string containing a unique sequence of characters can serve as a unique file
identifier for the DTS. For example, the following identifiers are all
technically valid:

* `615a383dcc4ff44f36ca5ba2`
* `machine-generated-id-21242452`
* `icanseemyhousefromhere`

The important thing is that the file can be uniquely identified. Users won't be
typing these identifiers in manually--they'll typically obtain them by searching
your database and selecting desired results.

The DTS assigns a database specific prefix to your identifier that gives it a
_namespace_ inside which it is unique. For example, the JGI Data Portal
identifier `615a383dcc4ff44f36ca5ba2` is made available by the DTS as
`JDP:615a383dcc4ff44f36ca5ba2`.

Sometimes a database consists of more than one dataset, and each dataset has its
own identifiers. For example, [Uniprot](https://www.uniprot.org/), one of
the world's largest collections of protein sequences, defines unique identifiers
for each of several distinct datasets. [This link](https://www.uniprot.org/help/linking_to_uniprot)
illustrates some examples of these dataset-specific identifiers.

Because Uniprot's identifiers indicate which dataset they're from, all of the
identifiers could be used within a single namespace by the DTS. This is one way
of providing access to multiple datasets. If this method of aggregating datasets
into a single namespace doesn't work for your organization, [the DTS team](mailto:engage@kbase.us)
is happy to discuss alternatives with you.

## File Metadata

Whatever file identification scheme your organization uses, you can provide the
DTS with the file information it needs by creating an endpoint that returns
file metadata for a given unique file identifier.

What's "metadata"? There is no universal answer to this question, of course. The
DTS is part of an effort to harmonize how our community searches for, obtains,
and cites scientific data, but the size and diversity of this community make it
difficult to find an answer even for our purposes alone!

Nevertheless, we press on. The metadata specification used by the DTS is likely
to undergo changes of all sorts as we figure things out, but we'll give our best
effort to updating documentation as these changes occur.

### DTS Metadata Specification

The DTS stores file metadata in the [Frictionless DataResource](https://specs.frictionlessdata.io/data-resource/)
format. This format is simple and relatively unopinionated, and allows for
additional fields as needed. The most important elements of the Frictionless
DataResource type are:

* `name`: the name of a resource (file)
* `path`: the path of the resource relative to the root directory of the
  [staging area](staging_area.md) in which you make the file available

The following additional fields are used by the DTS:

* `id`: your organization's unique identifier for the resource
* `credit`: credit metadata associated with the resource that conforms to the
  [KBase credit metadata schema](https://github.com/kbase/credit_engine)
* `metadata`: an optional unѕtructured field that you can use to stash
  additional information about the resource if needed. For now, the DTS does not
  use this field.

_It might be good to show an example here._

By the way, the DTS uses the [Frictionless DataPackage](https://specs.frictionlessdata.io/data-package/)
format to store file manifests for bulk file transfers. A data package is just
a collection of data resources with some additional metadata that applies to
all of them. A file manifest is generated automatically by the DTS after each
successful transfer.

If you adopt the Frictionless DataResource format for your own file metadata,
integration with the DTS will be very easy. If your organization already has its
own metadata format, [the DTS team can work with you](mailto:engage@kbase.us) to
determine how it can be translated to the Frictionless format for use by the
DTS.

## Endpoint Recommendations

Create a REST endpoint that accepts an HTTP `GET` request with a list of unique
file identifiers specified (for example) by a comma-separated set of request
parameters. This endpoint responds with a body containing a JSON list of
objects representing [Frictionless DataResources](https://specs.frictionlessdata.io/data-resource/)
describing the requested files in as much detail as is practical.

Error codes should be used in accordance with HTTP conventions:

* A successful query returns a `200 OK` status code
* An improperly-formed request should result in a `400 Bad Request` status code
* If one or more file IDs do not correspond to existing files in your
  organization's database, their entries in the JSON list in the response should
  be set to `null`.

### Example

Suppose we want to retrieve metadata for a couple of files provided by the
[JGI Data Portal](https://data.jgi.doe.dov) (JDP) with the unique identifiers
`615a383dcc4ff44f36ca5ba2` and `61412246cc4ff44f36c8913f` (referred to by the
DTS as `JDP:615a383dcc4ff44f36ca5ba2` and `JDP:61412246cc4ff44f36c8913f`,
respectively). If the JDP endpoint is hosted at `example.com` and is implemented
according to our recommendations, we can send the following `GET` request:

```
https://example.com/dts/resources?ids=615a383dcc4ff44f36ca5ba2,61412246cc4ff44f36c8913f
```

This results in a response with a `200 OK` status code with the body

```
[
  {
    "id": "JDP:615a383dcc4ff44f36ca5ba2",
    "name": "3300047546_18",
    "path": "img/submissions/255985/3300047546_18.tar.gz",
    "format": "tar",
    "media_type": "application/x-tar",
    "bytes": 1455012,
    "hash": "148827a6c3da2eaa4188b931ae0edc15",
    "sources": [
      {
        "title": "Wrighton, Kelly (Colorado State University, United States)",
        "path": "https://doi.org/10.46936/10.25585/60001289",
        "email": "kwrighton@gmail.com"
      }
    ],
    "credit": {
      "comment": "",
      "description": "",
      "identifier": "615a383dcc4ff44f36ca5ba2",
      "license": "",
      "resource_type": "dataset",
      "version": "",
      "contributors": [
        {
          "contributor_type": "Person",
          "contributor_id": "",
          "name": "Wrighton, Kelly",
          "credit_name": "Wrighton, Kelly",
          "affiliations": [
            {
              "organization_id": "",
              "organization_name": "Colorado State University"
            }
          ],
          "contributor_roles": "PI"
        }
      ],
      "dates": [
        {
          "date": "2019-09-17",
          "event": "approval"
        }
      ],
      "funding": null,
      "related_identifiers": null,
      "repository": {
        "organization_id": "",
        "organization_name": ""
      },
      "titles":null
    }
  },
  {
    "id": "JDP:61412246cc4ff44f36c8913f",
    "name": "3300047546",
    "path": "img/submissions/255985/3300047546.tar.gz",
    "format": "tar",
    "media_type": "application/x-tar",
    "bytes": 709508155,
    "hash": "fefe05140b0cad4e5d525acb066439e5",
    "sources": [
      {
        "title":"Wrighton, Kelly (Colorado State University, United States)",
        "path":"https://doi.org/10.46936/10.25585/60001289",
        "email":"kwrighton@gmail.com"
      }
    ],
    "credit": {
      "comment": "",
      "description": "",
      "identifier": "61412246cc4ff44f36c8913f",
      "license": "",
      "resource_type": "dataset",
      "version": "",
      "contributors": [
        {
          "contributor_type": "Person",
          "contributor_id": "",
          "name":"Wrighton, Kelly",
          "credit_name": "Wrighton, Kelly",
          "affiliations": [
            {
              "organization_id": "",
              "organization_name": "Colorado State University"
            }
          ],
          "contributor_roles": "PI"
        }
      ],
      "dates": [
        {
          "date": "2019-09-17",
          "event": "approval"
        }
      ],
      "funding":null,
      "related_identifiers": null,
      "repository": {
        "organization_id": "",
        "organization_name": ""
      },
      "titles":null
    }
  },
]
```

As you can see, many fields are blank, because some information may not be
present in the database. The DTS works with the [KBase Credit Engine](https://github.com/kbase/credit_engine)
to fill in missing credit information.

### Existing implementations

As it happens, the JDP doesn't provide its own file metadata endpoint, so the
DTS has its own logic for querying the JDP's source of truth. The DTS can bridge
gaps in the capabilities of the databases of participating organizations in this
way, making it easier for you to gradually implement a conforming capability.
