# Overview

The Data Transfer Service (DTS) is a web service that handles requests for file
transfers between participating organizations interested in exchanging
data. The DTS coordinates provides a single point of access for these
organizations, allowing an end user or another service to

* search for datasets / files within any participating organization based on
  criteria specified in an [ElasticSearch](https://www.elastic.co/elasticsearch/)
  style query
* select any or all files from a search and request a transfer from the source
  organization to another participating organization

DTS is designed for easy deployment and maintenance behind a gateway that
provides TLS/SSL encryption. Requests to the DTS include headers with
authentication information, so these requests rely on the HTTPS protocol to
protect this information.

It's very easy to deploy the DTS in a Docker environment and configure it using
environment variables.

# Contents

* [Administrator Guide](admin/index.md): How to configure and deploy the DTS
* [Integration Guide](integration/index.md): How to hook your organization's
  database up to the DTS to take advantage of its capabilities
* [Developer Guide](developer/index.md): A detailed description of the DTS
  code structure and important concepts
