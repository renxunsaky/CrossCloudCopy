package main

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type CopyMetaInfo struct {
	srcClient *s3.S3
	uploader  *s3manager.Uploader
	srcObject *s3.Object
	srcBucket *string
	dstBucket *string
	dstPrefix *string
}
