# Globus S3 Endpoint Demo

Prototype S3 endpoint for Globus-accesible data stores.

## Globus S3 API Server

You will need the following environment variables to be set:
* `DTS_GLOBUS_CLIENT_ID` User UUID
* `DTS_GLOBUS_CLIENT_SECRET` User secret

### Build
```
go build -o bin/globus_s3_server .
```

### Run
```
./bin/globus_s3_server
```

## Minio S3 client

The example runs using AWS and a Minio instance, which must be running locally. To start the Minio instance in detached mode, run:

```bash
docker run -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" -d minio/minio server /data --console-address ":9001"
```

Note the container id output after it starts up. You can stop the container when you're done using MinIO with:
```bash
docker stop <container-id>
```

In the `s3_client/` folder is a simple test application that attempts to transfer files from the Globus test endpoint into a MinIO bucket.

## Build
```
cd s3_client
go build -o ../bin/s3_client .
```

### Run
```
../bin/s3_client
```