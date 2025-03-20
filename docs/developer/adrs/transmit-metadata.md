# Transmitting Metadata

## Status

Accepted 19 March, 2025.

## Context

NMDC biosample metadata, which is associated with studies, is not stored in data
objects. Instead, one can fetch it by making a `GET` request to an API endpoint,
specifying the associated study or studies. The endpoint returns biosample
metadata encoded in a JSON object. When a user requests that the DTS transfer
NMDC data object files, the DTS queries this endpoint for the studies associated
with the files and includes it as [inline data in a Frictionless Data Resource](https://specs.frictionlessdata.io/data-resource/#data-location)
within the DTS manifest.

Biologists often expect this data to appear in a spreadsheet, which is more
easily inspectable by (most) humans. The team discussed whether the metadata
should be stored within the DTS manifest as inline JSON or CSV data.

## Decision

We decided to store inline metadata in JSON form within the DTS manifest. JSON
is a ubiquitous and flexible format for conveying structured data across the
internet, and can trivially be converted to other formats, including CSV data.
Other formats, like CSV, are either equivalent to JSON or more specific and
incapable of representing arbitrarily structured data.

## Consequences

This particular decision adds no friction to our current approach, since
the DTS uses Frictionless data structures for its manifests and resource
descriptors, and these data structures are easily represented in JSON.
