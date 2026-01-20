// Copyright (c) 2026 The KBase Project and its Contributors
// Copyright (c) 2026 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	minioEndpoint  = "http://localhost:9000"
	globusS3Endpoint = "http://localhost:8080"
	globusTestDirectory = "AmRNmKXYQjLBxrnB"
)

var (
	globusBucketId   = getGlobusBucketId()
)

func getGlobusBucketId() string {
	bucketId := os.Getenv("DTS_GLOBUS_TEST_ENDPOINT")
	if bucketId == "" {
		log.Fatal("DTS_GLOBUS_TEST_ENDPOINT environment variable is not set")
	}
	return bucketId
}

func transferFiles() error {
	// Get MinIO S3 client
	minioClient, err := getMinioS3Client()
	if err != nil {
		return err
	}

	// Get Globus S3 client
	globusClient, err := getGlobusS3Client()
	if err != nil {
		return err
	}

	// List buckets in MinIO and Globus S3 to verify connections
	minioBuckets, err := minioClient.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		return err
	}

	globusBuckets, err := globusClient.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		return err
	}

	// Print bucket names for verification
	if len(minioBuckets.Buckets) == 0 {
		log.Println("No buckets found in MinIO.")
	}
	for _, bucket := range minioBuckets.Buckets {
		fmt.Printf("MinIO Bucket: %s\n", aws.ToString(bucket.Name))
	}
	if len(globusBuckets.Buckets) == 0 {
		log.Println("No buckets found in Globus S3.")
	}
	for _, bucket := range globusBuckets.Buckets {
		fmt.Printf("Globus S3 Bucket: %s\n", aws.ToString(bucket.Name))
	}

	// List the contents of the Globus S3 bucket
	log.Printf("Getting objects from Globus S3 Bucket: %s\n", globusBucketId)
	globusObjects, err := globusClient.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(globusBucketId),
	})
	if err != nil {
		return err
	}

	log.Printf("Objects in Globus S3 Bucket %s:\n", globusBucketId)
	for _, obj := range globusObjects.Contents {
		log.Printf(" - %s (size: %d)\n", aws.ToString(obj.Key), aws.ToInt64(obj.Size))
	}

	log.Printf("Directories in Globus S3 Bucket %s:\n", globusBucketId)
	for _, dir := range globusObjects.CommonPrefixes {
		log.Printf(" - %s\n", aws.ToString(dir.Prefix))
	}

	log.Printf("Getting objects in Globus S3 Bucket %s/%s:\n", globusBucketId, globusTestDirectory)
	globusObjectsInDir, err := globusClient.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(globusBucketId),
		Prefix:    aws.String(globusTestDirectory + "/"),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return err
	}

	log.Printf("Objects in Globus S3 Bucket %s/%s:\n", globusBucketId, globusTestDirectory)
	for _, obj := range globusObjectsInDir.Contents {
		log.Printf(" - %s (size: %d)\n", aws.ToString(obj.Key), aws.ToInt64(obj.Size))
	}

	log.Printf("Directories in Globus S3 Bucket %s/%s:\n", globusBucketId, globusTestDirectory)
	for _, dir := range globusObjectsInDir.CommonPrefixes {
		log.Printf(" - %s\n", aws.ToString(dir.Prefix))
	}

	// // Create a local directory to hold downloaded files
	// err = createLocalDir("downloaded_files")
	// if err != nil {
	// 	return err
	// }

	// // dowload the /1M.dat file to the local directory
	// getObjOutput, err := globusClient.GetObject(context.TODO(), &s3.GetObjectInput{
	// 	Bucket: aws.String(globusBucketId),
	// 	Key:    aws.String("1M.dat"),
	// })
	// if err != nil {
	// 	return err
	// }
	// defer getObjOutput.Body.Close()

	// localFilePath := "downloaded_files/1M.dat"
	// localFile, err := createLocalFile(localFilePath)
	// if err != nil {
	// 	return err
	// }
	// defer localFile.Close()

	// _, err = io.Copy(localFile, getObjOutput.Body)
	// if err != nil {
	// 	return err
	// }
	// log.Printf("Downloaded '1M.dat' to '%s'\n", localFilePath)

	return nil
}

func getMinioS3Client() (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

    endpoint := minioEndpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.Credentials = credentials.NewStaticCredentialsProvider(
			"minioadmin", // Access Key
			"minioadmin", // Secret Key
			"",           // Token
		)
		o.Region = "us-east-1"
		o.UsePathStyle = true // Use path-style addressing
	})

	return s3Client, nil
}

func getGlobusS3Client() (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	endpoint := globusS3Endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.Credentials = credentials.NewStaticCredentialsProvider(
			"globus-s3-access-key", // Access Key
			"globus-s3-secret-key", // Secret Key
			"",                     // Token
		)
		o.Region = "us-east-1"
		o.UsePathStyle = true // Use path-style addressing
	})

	return s3Client, nil
}

func main() {
	err := transferFiles()
	if err != nil {
		log.Fatalf("File transfer failed: %s", err.Error())
	}
	log.Println("File transfer completed successfully.")
}

func createLocalDir(dirName string) error {
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		return fmt.Errorf("unable to create local directory '%s': %v", dirName, err)
	}
	return nil
}

func createLocalFile(filePath string) (*os.File, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to create local file '%s': %v", filePath, err)
	}
	return file, nil
}

