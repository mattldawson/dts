# Globus S3 Endpoint Demo

Prototype S3 endpoint for Globus-accesible data stores.

You will need the following environment variables to be set:
* `DTS_GLOBUS_CLIENT_ID` User UUID
* `DTS_GLOBUS_CLIENT_SECRET` User secret

## Build
```
go build -o bin/globus_s3_server .
```

## Run
```
./bin/globus_s3_server
```
