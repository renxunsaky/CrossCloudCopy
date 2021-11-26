package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"strings"
)

func GetStorageInfoFromUrl(url *string) (*string, *string, *string) {
	sourceArray := strings.Split(*url, "/")
	protocol := strings.TrimSuffix(sourceArray[0], ":")
	bucket := sourceArray[2]
	prefix := strings.Join(sourceArray[3:], "/")
	return &protocol, &bucket, &prefix
}

func GetStorageClientAndBucketInfo(url *string) (*s3.S3, *string, *string) {
	protocol, bucket, prefix := GetStorageInfoFromUrl(url)
	var client *s3.S3
	switch strings.ToLower(*protocol) {
	case "oss":
		client = GetOSSClient()
	case "s3":
		client = GetS3Client()
	case "gs":
		client = GetGSClient()
	}

	return client, bucket, prefix
}

func GetS3Client() *s3.S3 {
	region, regionPresent := os.LookupEnv("AWS_REGION")
	if !regionPresent {
		region = "eu-west-1"
	}
	profile, profilePresent := os.LookupEnv("AWS_PROFILE")
	var sess *session.Session
	if profilePresent {
		sess, _ = session.NewSessionWithOptions(session.Options{
			Config:  aws.Config{Region: &region},
			Profile: profile,
		})
	} else {
		accessKey := os.Getenv("AWS_ACCESS_KEY")
		accessSecret := os.Getenv("AWS_ACCESS_SECRET")
		sess, _ = session.NewSession(&aws.Config{
			Region:      &region,
			Credentials: credentials.NewStaticCredentials(accessKey, accessSecret, ""),
		})
	}

	return GetStorageClient(sess)
}

func GetOSSClient() *s3.S3 {
	ossEndPoint, present := os.LookupEnv("ALI_OSS_END_POINT")
	if !present {
		ossEndPoint = "oss-accelerate.aliyuncs.com"
	}
	profile, profilePresent := os.LookupEnv("ALI_PROFILE")
	var sess *session.Session
	if profilePresent {
		sess, _ = session.NewSessionWithOptions(session.Options{
			Config:  aws.Config{Region: aws.String("oss"), Endpoint: aws.String(ossEndPoint)},
			Profile: profile,
		})
	} else {
		accessKey := os.Getenv("ALI_ACCESS_KEY")
		accessSecret := os.Getenv("ALI_ACCESS_SECRET")
		sess, _ = session.NewSession(&aws.Config{
			Region:      aws.String("oss"),
			Endpoint:    aws.String(ossEndPoint),
			Credentials: credentials.NewStaticCredentials(accessKey, accessSecret, ""),
		})
	}

	return GetStorageClient(sess)
}

func GetGSClient() *s3.S3 {
	gsEndPoint, present := os.LookupEnv("GCP_OSS_END_POINT")
	if !present {
		gsEndPoint = "oss-accelerate.aliyuncs.com"
	}
	profile, profilePresent := os.LookupEnv("GCP_PROFILE")
	var sess *session.Session
	if profilePresent {
		sess, _ = session.NewSessionWithOptions(session.Options{
			Config:  aws.Config{Region: aws.String("gs"), Endpoint: aws.String(gsEndPoint)},
			Profile: profile,
		})
	} else {
		accessKey := os.Getenv("GCP_ACCESS_KEY")
		accessSecret := os.Getenv("GCP_ACCESS_SECRET")
		sess, _ = session.NewSession(&aws.Config{
			Region:      aws.String("oss"),
			Endpoint:    aws.String(gsEndPoint),
			Credentials: credentials.NewStaticCredentials(accessKey, accessSecret, ""),
		})
	}
	return GetStorageClient(sess)
}

func GetStorageClient(session *session.Session) *s3.S3 {
	return s3.New(session)
}

func GetDstObjectKey(srcKey *string, dstPrefix *string) *string {
	var finalKey string
	if strings.HasSuffix(*dstPrefix, "/") {
		srcKeyArray := strings.Split(*srcKey, "/")
		finalKey = *dstPrefix + srcKeyArray[len(srcKeyArray)-1]
		return &finalKey
	} else {
		return dstPrefix
	}
}

func GetUploader(dstClient *s3.S3) *s3manager.Uploader {
	uploader := s3manager.NewUploaderWithClient(dstClient, func(u *s3manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB per part
		u.Concurrency = 20
	})
	return uploader
}

func ExitError(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
