# DTS Configuration File Specification

You can configure a DTS instance by creating a [YAML](https://yaml.org/) text
file similar to [dts.yaml.example](https://github.com/kbase/dts/blob/main/dts.yaml.example)
in the repository. Typically this file is named `dts.yaml`, and is passed as an
argument to the `dts` executable. Here we describe the different sections in
this file and how they affect your DTS instance.

## Configuration File Sections

Click on any of the links below to see the relevant details for a section.

* [service](config.md#service): configureѕ settings for the DTS web service
  such as the port on which it listens, the maximum number of connections,
  intervals for polling and scrubbing completed tasks, data directories, and
  diagnostics
* [endpoints](config.md#endpoints): configures the endpoints used to transfer
  files from one place to another
* [databases](config.md#databases): configures databases for organizations that
  integrate with the DTS

Each of these sections is described below, with a motivating example.

## `service`

```yaml
service:
  port: 8080
  max_connections: 100
  poll_interval:   60000
  endpoint: globus-local
  data_dir: /path/to/dir
  delete_after: 604800
  debug: true
```

**TODO: write some stuff!**

## `endpoints`

```yaml
endpoints:
  globus-local:
    name: name-of-local-endpoint
    id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    provider: globus
    auth:
      client_id: <ID of client with authentication secret>
      client_secret: <secret>
  globus-jdp:
    name: name-of-jdp-endpoint
    id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    provider: globus
    auth:
      client_id: <ID of client with authentication secret>
      client_secret: <secret>
  globus-kbase:
    name: name-of-kbase-endpoint
    id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    provider: globus
    auth:
      client_id: <ID of client with authentication secret>
      client_secret: <secret>
```

**TODO: Things and stuff**

## `databases`

```yaml
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    endpoint: globus-jdp
  kbase:
    name: KBase Workspace Service (KSS)
    organization: KBase
    endpoint: globus-kbase
```

**TODO: Alll the things**

