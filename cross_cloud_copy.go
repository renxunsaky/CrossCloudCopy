package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"os"
	"runtime"
	"sync"
)

func main() {
	if len(os.Args) != 3 {
		ExitError("Bucket name required\nUsage: %s bucket_name", os.Args[0])
	}

	source := os.Args[1]
	target := os.Args[2]
	srcClient, srcBucket, srcPrefix := GetStorageClientAndBucketInfo(&source)
	dstClient, desBucket, dstPrefix := GetStorageClientAndBucketInfo(&target)

	// Get the list of items from source
	resp, err := srcClient.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: srcBucket,
		Prefix: srcPrefix,
	})
	if err != nil {
		ExitError("Unable to list items in bucket %q, %v", srcBucket, err)
	}

	uploader := GetUploader(dstClient)

	var wg sync.WaitGroup
	for _, item := range resp.Contents {
		wg.Add(1)
		go CopyObject(&wg, srcClient, uploader, *item, srcBucket, desBucket, dstPrefix)
	}
	wg.Wait()
	fmt.Println("Copy finished !")
}

func CopyObject(wg *sync.WaitGroup, srcClient *s3.S3, uploader *s3manager.Uploader, obj s3.Object,
	srcBucket *string, dstBucket *string, dstPrefix *string) {
	defer wg.Done()
	//size := *obj.Size

	fmt.Printf("Launching copying of %s\n", *obj.Key)
	fmt.Printf("Current goroutines %d", runtime.NumGoroutine())
	objResp, err := srcClient.GetObject(&s3.GetObjectInput{
		Bucket: srcBucket,
		Key:    obj.Key,
	})
	if err != nil {
		ExitError("Unable to get object in bucket %q, %v", srcBucket, err)
	} else {
		UploadObject(uploader, objResp, dstBucket, GetDstObjectKey(obj.Key, dstPrefix))
	}
	fmt.Printf("Finished copying of %s\n", *obj.Key)
}

func UploadObject(uploader *s3manager.Uploader, objResp *s3.GetObjectOutput, dstBucket *string, dstKey *string) {
	content, _ := ioutil.ReadAll(objResp.Body)
	uploadOutput, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: dstBucket,
		Key:    dstKey,
		Body:   bytes.NewReader(content),
	})

	if err != nil {
		ExitError("Unable to upload object in bucket %q, %v", *dstBucket, err)
	} else {
		fmt.Printf("Uploaded %v, ETag is %v\n", uploadOutput.Location, *uploadOutput.ETag)
	}
}
