# Provide a Way to Monitor File Staging

In the last section, we described the file staging process, in which the Data
Transfer System (DTS) asks your system to stage a set of files for transfer.
The DTS makes a file staging request, and your system returns some sort of
identifier that can be used to track the staging operation. In this section,
we describe a _staging status endpoint_ that the DTS can query with the
identifier it was given.

This one's easy--your endpoint should accept a UUID that the DTS was given when
it requested a staging operation for a set of specific files, and return
information about the status of that operation.

## Endpoint Recommendations

Create a REST endpoint that accepts an HTTP `GET` request with a UUID string
received from a prior `POST` request for a file staging operation. The endpoint
responds with a body containing a `status` field with a string value conveying
that the files are `ready`, or `staging` (for example).

Error codes should be used in accordance with HTTP conventions:

* A successful query returns a `200 OK` status code
* An improperly-formed request should result in a `400 Bad Request` status code
* If the UUID provided in the request doesn't match any ongoing or completed
  file staging operation, your endpoint should return a `404 Not Found` status
  code.

### Example

Suppose now that we want to retrieve the status for the [JGI Data Portal](https://data.jgi.doe.gov)
file staging operation we requested in the last section
(UUID: `4b86e181-8c83-447e-aada-af9232af7da0`). If the JDP endpoint is hosted at
`example.com` and is implemented according to our recommendations, we can send
the following `GET` request:

```
https://example.com/dts/staging?id=4b86e181-8c83-447e-aada-af9232af7da0
```

This results in a response with a `200 OK` status code with the body

```
{
  "status": "ready"
}
```

This response conveys to the DTS that the requested files have been copied into
place within the file staging area, so their transfer can begin.

### Existing implementations

The JDP's [file staging status endpoint](https://files.jgi.doe.gov/apidoc/#/GET/request_archived_files_requests_read)
returns a very elaborate set of status information, most of which is discarded
by the DTS. As we've discussed in previous sections, the DTS prototype
implementation repurposes a lot of existing endpoints for the JDP, and we are
prepared to work with your organization to determine how best to wire your
existing system into the DTS.

