# Make Your Files Searchable

If you've established unique identifiers for your organization's files and
implemented an endpoint to [provide unique IDs and metadata for them](resources.md),
your next step is to allow BER researchers to search for files of interest so
they can select which ones they would like to move around.

Search technology has been around a long time and has gotten pretty
sophisticated, with diverse options to meet diverse needs. Some interesting
options for implementing your own search capability are (in alphabetical order):

* [Amazon CloudSearch](https://aws.amazon.com/cloudsearch/)
* [Apache Solr](https://solr.apache.org/)
* [Elasticsearch](https://www.elastic.co/elasticsearch)
* [OpenSearch](https://opensearch.org/)
* [Meilisearch](https://www.meilisearch.com/)
* [Postgres FTS](https://www.postgresql.org/docs/current/textsearch.html)
* [Redis Search](https://developer.redis.com/modules/redisearch/)
* [Typesense](https://typesense.org/)

Your organization may use one or a few of these options already. Each of these
capabilities has its own strengths and weaknesses, based on the purpose for
which it was conceived and built. The structure and content of your database
can help you determine which is most appropriate.

The DTS does not currently embrace any one search engine at the expense of
others. Our team can work with you to make sure that your integration exposes
all the necessary features of your file search capability.

## Endpoint Recommendations

Create a REST endpoint that accepts an HTTP `GET` request with a set of request
parameters providing

* an appropriate query string that your database can use to search for files
* pagination parameters that allow search results to be broken into sets of
  manageable sizes for inspection by a human being, e.g.
    * the maximum number of results to return for a single request ("results
      per page")
    * the offset of the first result to display among all results ("the starting
      page")
* whatever other information your search engine may need to do its thing

The endpoint responds with a body containing a JSON list of objects representing
[Frictionless DataResources](https://specs.frictionlessdata.io/data-resource/)
describing the files that match the given search query in as much detail as is
practical.

Error codes should be used in accordance with HTTP conventions:

* A successful query returns a `200 OK` status code
* An improperly-formed request should result in a `400 Bad Request` status code

### Example

The [JGI Data Portal](https://data.jgi.doe.gov/) (JDP) uses ElasticSearch to
implement its file search capability.

At the time of writing, this query produces well over 100 results. Here are
the first two results, expressed as Frictionless DataResource objects in JSON:

```
[
  {
    "id": "JDP:57f9e03f7ded5e3135bc069e",
    "name": "10927.1.183804.CTCTCTA-AGGCTTA.QC",
    "path": "rqc/10927.1.183804.CTCTCTA-AGGCTTA.QC.pdf",
    "format": "pdf",
    "bytes": 227745,
    "hash": "71a60d25af7b35227e8b0f3428b49687",
    "sources": [
      {
        "title": "Stewart, Frank (Georgia Institute of Technology, United States)",
        "path": "https://doi.org/10.46936/10.25585/60000893",
        "email": "frank.stewart@biology.gatech.edu"
      }
    ],
    "credit": {
      "comment": "",
      "description": "",
      "identifier": "JDP:57f9e03f7ded5e3135bc069e",
      "license": "",
      "resource_type": "dataset",
      "version": "",
      "contributors": [
        {
          "contributor_type": "Person",
          "contributor_id": "",
          "name": "Stewart, Frank",
          "credit_name": "Stewart, Frank",
          "affiliations": [
            {
              "organization_id": "",
              "organization_name": "Georgia Institute of Technology"
            }
          ],
          "contributor_roles": "PI"
        }
      ],
      "dates": [
        {
          "date": "2013-09-20",
          "event": "approval"
        }
      ],
      "funding": null,
      "related_identifiers": null,
      "repository": {
        "organization_id": "",
        "organization_name": ""
      },
      "titles": null
    }
  },
  {
    "id": "JDP:57f9d2b57ded5e3135bc0612",
    "name": "10927.1.183804.CTCTCTA-AGGCTTA.filter-SAG",
    "path": "rqc/filtered_seq_unit/00/01/09/27/10927.1.183804.CTCTCTA-AGGCTTA.filter-SAG.fastq.gz",
    "format": "fastq",
    "media_type": "text/plain",
    "bytes":2225019092,
    "hash": "55715364087c8553c99c126231e599dc",
    "sources":[
      {
        "title": "Stewart, Frank (Georgia Institute of Technology, United States)",
        "path": "https://doi.org/10.46936/10.25585/60000893",
        "email": "frank.stewart@biology.gatech.edu"
      }
    ],
    "credit": {
      "comment": "",
      "description": "",
      "identifier": "JDP:57f9d2b57ded5e3135bc0612",
      "license": "",
      "resource_type": "dataset",
      "version": "",
      "contributors":[
        {
          "contributor_type": "Person",
          "contributor_id": "",
          "name": "Stewart, Frank",
          "credit_name": "Stewart, Frank",
          "affiliations": [
            {
              "organization_id": "",
              "organization_name": "Georgia Institute of Technology"
            }
          ],
          "contributor_roles": "PI"
        }
      ],
      "dates": [
        {
          "date": "2013-09-20",
          "event": "approval"
        }
      ],
      "funding": null,
      "related_identifiers":null,
      "repository": {
        "organization_id": "",
        "organization_name": ""
      },
      "titles":null
    }
  },
  ...
]
```

Many fields are blank, particularly in the `credit` field, because some of this
information isn't in the JDP database. The DTS works with the [KBase Credit Engine](https://github.com/kbase/credit_engine)
to fill in missing credit information.

## Existing implementations

The [JDP search endpoint](https://files.jgi.doe.gov/apidoc/#/GET/search_list)
we've described in the above example returns information about the files
matching a query, but not in the Frictionless format we've shown. The JDP
actually organizes its search results into organisms, and the DTS unpacks these
results and repackages them into Frictionless DataResource objects. This is
another example of how the DTS team can support incremental integration by
working with your organization.
