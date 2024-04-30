# Stage Your Files on Request

After you've [designated a file staging area and configured a Globus guest
collection](staging_area.md) for your organization, it's time to set up a
_staging request endpoint_ that allows the Data Transfer System (DTS) to ask for
specific files to be moved to this staging area in preparation for transfer.

Why is this endpoint needed? In a perfect world, storage would be infinite and
free, and your organization could simply "keep all the files somewhere, ready to
be transferred anytime." In practice, the amount of scientific data being
continuously generated greatly exceeds available storage at any given moment,
so it needs to be archived when it's not being used.

For example, the JGI Data Portal (JDP) stores most of its data on tape,
unarchiving it to disk (the staging area used by the DTS, specifically) on
request. Your organization probably has a similar scheme in place for storing
its data.

It works like this: when a user searches for and selects files to transfer from
your database to somewhere else, the DTS checks your staging area for the
requested files via Globus. If those files are not in your staging area, the DTS
makes a request to the staging request endpoint to ask your system to copy the
files into place, whatever that entails.

The DTS understands that this process may take some time, so it also needs a way
to request the status of your file staging operation. After all, there are only
so many robot arms in a tape backup facility, and they can only swing around so
quickly and move so many tapes at once. Accordingly, your system must provide
a unique identifier for the file staging operation that allows the DTS to check
for its completion.

Let's take a look at the staging request endpoint. We'll discuss how your system
can report the status of a file staging operation in the next section.

## Endpoint Recommendations

Create a REST endpoint that accepts an HTTP `POST` request with a body that
contains a set of unique identifiers corresponding to files that should be moved
to your file staging area. The endpoint validates the request, checking that the
files exist in your system, and initiate a staging operation that copies the
requested files into place within your staging area. The endpoint then responds
with a body containing a [universally unique identifier](https://en.wikipedia.org/wiki/Universally_unique_identifier)
(UUID) that the DTS can use to request the status of the staging operation.

Error codes should be used in accordance with HTTP conventions:

* A successful query returns a `201 Created` status code
* An improperly-formed request should result in a `400 Bad Request` status code
* If one or more file IDs do not correspond to existing files in your
  organization's database, the endpoint can respond with a `404 Not Found`
  status code.

### Example

Suppose we want to request that [JGI Data Portal](https://data.jgi.doe.dov)
(JDP) stage the files with the unique identifiers `615a383dcc4ff44f36ca5ba2` and
`61412246cc4ff44f36c8913f` (referred to by the DTS as
`JDP:615a383dcc4ff44f36ca5ba2` and `JDP:61412246cc4ff44f36c8913f`,
respectively). If the JDP endpoint is hosted at `example.com` and is implemented
according to our recommendations, we can send a `POST` to

```
https://example.com/dts/staging
```

with the following body:

```
{
  "ids": [
    "615a383dcc4ff44f36ca5ba2",
    "61412246cc4ff44f36c8913f"
  ]
}
```

This results in a response with a `201 Created` status code with a body
containing a UUID:

```
{
  "request_id": "4b86e181-8c83-447e-aada-af9232af7da0"
}
```

We'll see how this UUID can be used to retrieve the status of the file staging
operation in the next section.

### Existing implementations

The [JDP endpoint](https://files.jgi.doe.gov/apidoc/#/POST/request_archived_files_create)
we mention above essentially conforms to what we've described, but contains some
additional fields that determine, for example, whether an email is sent upon
completion of the staging process. This is because we've repurposed an endpoint
that was originally intended to allow users to download requested files directly
from Globus. If your organization already has something in place that can serve
as a file staging request endpoint, we can work with you to similarly leverage
it to get your system hooked up to the DTS.
