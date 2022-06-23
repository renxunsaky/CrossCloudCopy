package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultMaxConcurrentGoroutines = 5
	DefaultMaxRetry                = 5
	DefaultAddSuccessFile          = false
	DefaultIsDeltaLake             = false
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
	isAddSuccessFile := DefaultAddSuccessFile
	isDeltaLake := DefaultIsDeltaLake
	if len(os.Args) > 3 {
		maxConcurrent, _ = strconv.Atoi(os.Args[3])
	}
	if len(os.Args) > 4 {
		maxRetry, _ = strconv.Atoi(os.Args[4])
	}
	if len(os.Args) > 5 {
		isAddSuccessFile, _ = strconv.ParseBool(os.Args[5])
	}
	if len(os.Args) > 6 {
		isDeltaLake, _ = strconv.ParseBool(os.Args[6])
	}
	srcClient, srcBucket, srcPrefix := GetStorageClientAndBucketInfo(&source)
	dstClient, desBucket, dstPrefix := GetStorageClientAndBucketInfo(&target)

	startTime := time.Now()
	// Get the list of items from source
	sourceSuccessFilePrefix := *srcPrefix + "_SUCCESS"
	_, headObjectOutputErr := srcClient.HeadObject(&s3.HeadObjectInput{
		Bucket: srcBucket,
		Key:    &sourceSuccessFilePrefix,
	})

	var sourceHasSuccessFile = false
	if headObjectOutputErr != nil {
		println("source bucket doesn't have the _SUCCESS file")
		println(headObjectOutputErr.Error())
	} else {
		sourceHasSuccessFile = true
	}

	fileContent := ""
	if isDeltaLake {
		var manifestErr error
		fileContent, manifestErr = ReadDeltaLakeManifestFile(srcClient, srcBucket, srcPrefix)
		if manifestErr != nil {
			ExitError("Error while reading DeltaLake manifest file", manifestErr)
		}
	}

	var continuationToken *string
	for {
		resp, err := srcClient.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:  srcBucket,
			Prefix:  srcPrefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			ExitError("Unable to list items in bucket %q, %v", srcBucket, err)
		}

		concurrentGoroutines := make(chan int, maxConcurrent)
		var wg sync.WaitGroup
		for i, item := range resp.Contents {
			if isDeltaLake && !strings.Contains(fileContent, *item.Key) {
				log.Printf("file %s not in DeltaLake manifest file, to be ignored\n", *item.Key)
				continue
			}
			wg.Add(1)
			obj := item
			go func(i int) {
				defer wg.Done()
				concurrentGoroutines <- i
				copyMetaInfo := &CopyMetaInfo{
					srcClient, dstClient,
					obj, srcBucket, desBucket, dstPrefix,
				}
				copyErr := Retry(maxRetry, 5*time.Second, CopyObject, copyMetaInfo)
				if copyErr != nil {
					ExitError("Error happened after max retry for %q, %v", *obj.Key, copyErr)
				} else {
					fmt.Printf("Copied %s with success\n", *obj.Key)
				}
				<-concurrentGoroutines
			}(i)
		}
		wg.Wait()

		if !aws.BoolValue(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Printf("Copy process finished ! Used total time: %.2f minutes\n", duration.Minutes())
	if isAddSuccessFile || sourceHasSuccessFile {
		var emptyContent []byte
		successFilePrefix := *dstPrefix + "_SUCCESS"
		_, err := dstClient.PutObject(&s3.PutObjectInput{
			Bucket: desBucket,
			Key:    &successFilePrefix,
			Body:   bytes.NewReader(emptyContent),
		})
		if err != nil {
			fmt.Printf("Error while adding _SUCCESS file")
		}
	}
}

func CopyObject(copyMetaInfo *CopyMetaInfo) error {
	fmt.Printf("Launching copying of %s\n", *copyMetaInfo.srcObject.Key)
	if *copyMetaInfo.srcObject.Size > DefaultPartSizeByte {
		return LaunchMultiPartUpload(copyMetaInfo)
	} else {
		if strings.HasSuffix(*copyMetaInfo.srcObject.Key, "_SUCCESS") {
			println("Ignore copying _SUCCESS here and will put it later")
			return nil
		} else {
			return LaunchSimpleUpload(copyMetaInfo)
		}
	}
}

func LaunchSimpleUpload(copyMetaInfo *CopyMetaInfo) error {
	dstKey := GetDstObjectKey(copyMetaInfo.srcObject.Key, copyMetaInfo.dstPrefix)
	objectOutput, getObjectErr := copyMetaInfo.srcClient.GetObject(&s3.GetObjectInput{
		Bucket: copyMetaInfo.srcBucket,
		Key:    copyMetaInfo.srcObject.Key,
	})
	if getObjectErr != nil {
		return getObjectErr
	}
	content, _ := ioutil.ReadAll(objectOutput.Body)
	md5Sum := base64Sum(content)
	_, putObjectErr := copyMetaInfo.dstClient.PutObject(&s3.PutObjectInput{
		Bucket:     copyMetaInfo.dstBucket,
		Key:        dstKey,
		Body:       bytes.NewReader(content),
		ContentMD5: &md5Sum,
	})
	if putObjectErr != nil {
		return putObjectErr
	} else {
		fmt.Printf("Using simple upload for %s", *dstKey)
		return nil
	}
}

func LaunchMultiPartUpload(copyMetaInfo *CopyMetaInfo) error {
	totalPartNumber := CalculatePartNumber(copyMetaInfo.srcObject.Size)
	dstKey := GetDstObjectKey(copyMetaInfo.srcObject.Key, copyMetaInfo.dstPrefix)
	uploadOutput, createMultipartUploadErr := copyMetaInfo.dstClient.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: copyMetaInfo.dstBucket,
		Key:    dstKey,
	})
	if createMultipartUploadErr != nil {
		return fmt.Errorf("unable to create multipart upload in bucket %q, %v", *copyMetaInfo.dstBucket, createMultipartUploadErr)
	}

	var wg sync.WaitGroup
	var parts completedParts
	for i := int64(1); i < totalPartNumber+1; i++ {
		wg.Add(1)
		partNumber := i
		startBytes := (partNumber - 1) * DefaultPartSizeByte
		endBytes := startBytes + DefaultPartSizeByte - 1
		go func() {
			defer wg.Done()
			part, err := ReadFromSourceAndWriteToDestination(copyMetaInfo, uploadOutput.UploadId, &partNumber, startBytes, endBytes)
			if err != nil {
				log.Println(err)
				fmt.Printf("Error happened during coping part %d of %s. Abording multipart\n", partNumber, *dstKey)
				_, _ = copyMetaInfo.dstClient.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
					UploadId: uploadOutput.UploadId,
					Bucket:   copyMetaInfo.dstBucket,
					Key:      dstKey,
				})
				return
			} else {
				parts = append(parts, part)
			}
		}()
	}
	wg.Wait()
	sort.Sort(parts)
	completeMultipartUploadRes, completeMultipartUploadErr := copyMetaInfo.dstClient.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          copyMetaInfo.dstBucket,
		Key:             dstKey,
		UploadId:        uploadOutput.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: parts},
	})

	if completeMultipartUploadErr == nil {
		fmt.Printf("Completed multiupload for %s, now checking data integrity... \n", *dstKey)
		HeadObjectOutput, HeadObjectErr := copyMetaInfo.dstClient.HeadObject(&s3.HeadObjectInput{
			Bucket: completeMultipartUploadRes.Bucket,
			Key:    completeMultipartUploadRes.Key,
		})
		if HeadObjectErr != nil {
			log.Println(HeadObjectErr)
			return HeadObjectErr
		} else {
			if *HeadObjectOutput.ContentLength != *copyMetaInfo.srcObject.Size {
				fmt.Printf("source object(%s) size(%d) different than destination object size(%d) \n",
					*copyMetaInfo.srcObject.Key, *copyMetaInfo.srcObject.Size, *HeadObjectOutput.ContentLength)
				return errors.New("source object size different than destination object size")
			} else {
				fmt.Printf("Data integrity checked for %s, it's OK with length of %d \n",
					*completeMultipartUploadRes.Key, *HeadObjectOutput.ContentLength)
				return nil
			}
		}
	} else {
		return completeMultipartUploadErr
	}
}

func ReadFromSourceAndWriteToDestination(copyMetaInfo *CopyMetaInfo, uploadId *string,
	partNumber *int64, startBytes int64, endBytes int64) (*s3.CompletedPart, error) {
	objRange := fmt.Sprintf("bytes=%d-%d", startBytes, endBytes)
	objOutPutRes, err := copyMetaInfo.srcClient.GetObject(&s3.GetObjectInput{
		Bucket: copyMetaInfo.srcBucket,
		Key:    copyMetaInfo.srcObject.Key,
		Range:  &objRange,
	})
	if err != nil {
		return nil, err
	} else {
		content, _ := ioutil.ReadAll(objOutPutRes.Body)
		md5Sum := base64Sum(content)
		fmt.Printf("uploading object [%s] with [partNum]: %d, [uploadID]: %s, [MD5]: %s \n",
			*copyMetaInfo.srcObject.Key, partNumber, *uploadId, md5Sum)
		uploadPartOutput, uploadPartErr := copyMetaInfo.dstClient.UploadPart(&s3.UploadPartInput{
			UploadId:   uploadId,
			PartNumber: partNumber,
			Bucket:     copyMetaInfo.dstBucket,
			Key:        GetDstObjectKey(copyMetaInfo.srcObject.Key, copyMetaInfo.dstPrefix),
			Body:       bytes.NewReader(content),
			ContentMD5: &md5Sum,
		})
		if uploadPartErr != nil {
			return nil, uploadPartErr
		} else {
			part := &s3.CompletedPart{ETag: uploadPartOutput.ETag, PartNumber: partNumber}
			fmt.Printf("uploaded object [%s] for [partNum]: %d with [uploadID]: %s, [ETag]: %s \n",
				*copyMetaInfo.srcObject.Key, *part.PartNumber, *uploadId, *part.ETag)
			return part, nil
		}
	}
}
