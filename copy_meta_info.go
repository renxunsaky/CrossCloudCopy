package main

import (
	"github.com/aws/aws-sdk-go/service/s3"
)

type CopyMetaInfo struct {
	srcClient *s3.S3
	dstClient *s3.S3
	srcObject *s3.Object
	srcBucket *string
	dstBucket *string
	dstPrefix *string
}
