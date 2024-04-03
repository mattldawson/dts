# Map ORCID IDs to Local User Accounts

DTS uses [ORCID IDs](https://info.orcid.org/what-is-orcid/) to identify
individuals and organizations. In order to understand who is transferring what
where when, your database must establish a connection between a user's ORCID ID
and their local account on your system. This connection is a form of
[federated identity management](https://en.wikipedia.org/wiki/Federated_identity),
similar to Single Sign-On (SSO) authentication services offered by various
platforms.

## Endpoint Recommendations

Create a REST endpoint that accepts an HTTP `GET` request with a given ORCID ID
as a path parameter or request parameter. This endpoint responds with a body
containing a JSON object with two fields:

* `orcid`: the ORCID ID passed as the query parameter
* `username`: the local username corresponding to the given ORCID ID

Error codes should be used in accordance with HTTP conventions:

* A successful query returns a `200 OK` status code
* An improperly-formed ORCID ID should result in a `400 Bad Request` status code
* An ORCID ID that does not correspond to a local user should produce `404 Not Found`

### Example

For example, suppose we want to find the local username for [Josiah Carberry](https://orcid.org/0000-0002-1825-0097),
a fictitious Professor of Psychoceramics at Brown University. The University's
federated identity endpoint provides the endpoint `https://example.com/dts/localuser/{orcid}`
that accepts a path parameter `{orcid}`. We can retrieve Josiah's local username
in the University's database by sending an HTTP `GET` request to

```
https://example.com/dts/localuser/0000-0002-1825-0097
```

This produces a responѕe with a `200 OK` status code with the body

```
{
  "orcid": "0000-0002-1825-0097",
  "username": "psyclay"
}
```

### Existing implementations

This particular feature is actually not yet supported by KBase, so the DTS
prototype uses a locally-stored JSON file containing an object whose
field names are ORCID IDs and whose values are usernames for the corresponding
KBase users.

* Example: current workaround (to be updated when JDP and KBase support this!)
