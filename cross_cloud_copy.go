package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	DefaultMaxConcurrentGoroutines = 5
	DefaultMaxRetry                = 5
)

func main() {
	if len(os.Args) < 3 {
		ExitError("Arguments incorrect\n"+
			"Example: %s s3://bucket_name/data/ oss://bucket_name/data/ [maxConcurrent] [maxRetry]", os.Args[0])
	}

	source := os.Args[1]
	target := os.Args[2]
	maxConcurrent := DefaultMaxConcurrentGoroutines
	maxRetry := DefaultMaxRetry
	if len(os.Args) > 3 {
		maxConcurrent, _ = strconv.Atoi(os.Args[3])
	}
	if len(os.Args) > 4 {
		maxRetry, _ = strconv.Atoi(os.Args[4])
	}
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

	concurrentGoroutines := make(chan int, maxConcurrent)
	var wg sync.WaitGroup

	for i, item := range resp.Contents {
		wg.Add(1)
		obj := item
		go func(i int) {
			defer wg.Done()
			concurrentGoroutines <- i
			copyMetaInfo := &CopyMetaInfo{
				srcClient, NewUploader(dstClient, obj),
				obj, srcBucket, desBucket, dstPrefix,
			}
			copyErr := Retry(maxRetry, 5*time.Second, CopyObject, copyMetaInfo)
			if copyErr != nil {
				ExitError("Error happened after max retry for %q, %v", *obj.Key, copyErr)
			}
			<-concurrentGoroutines
		}(i)
	}
	wg.Wait()
	fmt.Println("Copy finished !")
}

func CopyObject(copyMetaInfo *CopyMetaInfo) error {
	fmt.Printf("Launching copying of %s\n", *copyMetaInfo.srcObject.Key)
	objResp, downloadErr := copyMetaInfo.srcClient.GetObject(&s3.GetObjectInput{
		Bucket: copyMetaInfo.srcBucket,
		Key:    copyMetaInfo.srcObject.Key,
	})
	if downloadErr != nil {
		return fmt.Errorf("unable to get object in bucket %q, %v", *copyMetaInfo.srcBucket, downloadErr)
	} else {
		uploadErr := UploadObject(copyMetaInfo.uploader, objResp, copyMetaInfo.dstBucket,
			GetDstObjectKey(copyMetaInfo.srcObject.Key, copyMetaInfo.dstPrefix))
		if uploadErr != nil {
			fmt.Printf("Finished copying of %s\n", *copyMetaInfo.srcObject.Key)
		}
		return uploadErr
	}
}

func UploadObject(uploader *s3manager.Uploader, objResp *s3.GetObjectOutput, dstBucket *string, dstKey *string) error {
	content, _ := ioutil.ReadAll(objResp.Body)
	uploadOutput, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: dstBucket,
		Key:    dstKey,
		Body:   bytes.NewReader(content),
	})

	if err != nil {
		return fmt.Errorf("unable to upload object in bucket %q, %v", *dstBucket, err)
	} else {
		fmt.Printf("Uploaded %v, ETag is %v\n", uploadOutput.Location, *uploadOutput.ETag)
		return nil
	}
}
