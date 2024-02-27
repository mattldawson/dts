# DTS Installation

Here we describe how to build and deploy the Data Transfer System in an
appropriate environment.

## Building and Testing DTS Locally

DTS is written in [Go](https://go.dev/), so you'll need a working Go compiler
to build, test, and run it locally. If you have a Go compiler, you can clone
this repository and build it from the top-level directory:

```
go build
```

### Running Unit Tests

DTS comes with several unit tests that demonstrate its capabilities, and you can
run these tests as you would any other Go project:

```
go test ./...
```

You can add a `-v` flag to see output from the tests.

Because DTS is primarily an orchestrator of network resources, its unit tests
must be able to connect to and utilize these resources. Accordingly, you must
set the following environment variables to make sure DTS can do what it needs
to do:

* `DTS_KBASE_DEV_TOKEN`: a KBase development token (available to
  [KBase developers](https://docs.kbase.us/development/create-a-kbase-developer-account)
  used to connect to the KBase Auth Server, which provides a context for
  authenticating and authorizing DTS for its basic operations
* `DTS_KBASE_TEST_ORCID`: an [ORCID](https://orcid.org/) identifier that can be
  used to run DTS's unit test. This identifier must match a registered ORCID ID
  associated with a [KBase user account](https://narrative.kbase.us/#signup).
* `DTS_KBASE_TEST_USER`: the KBase user associated with the ORCID specified
  by `DTS_KBASE_TEST_ORCID`.
* `DTS_GLOBUS_CLIENT_ID`: a client ID registered using the
  [Globus Developers](https://docs.globus.org/globus-connect-server/v5/use-client-credentials/#register-application)
  web interface. This ID must be registered specifically for an instance of DTS.
* `DTS_GLOBUS_CLIENT_SECRET`: a client secret associated with the client ID
  specified by `DTS_GLOBUS_CLIENT_ID`
* `DTS_GLOBUS_TEST_ENDPOINT`: a Globus endpoint used to test DTS's transfer
  capabilities
* `DTS_JDP_SECRET`: a string containing a shared secret that allows the DTS to
  authenticate with the JGI Data Portal

## Deploying in a Docker Container

**TODO: More to come**
