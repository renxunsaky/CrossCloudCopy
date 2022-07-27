package main

import (
	_ "container/list"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
	"time"
)

const (
	DefaultPartSizeByte = int64(10 * 1024 * 1024)
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
		gsEndPoint = "storage.googleapis.com"
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

func GetDstObjectKey(srcKey *string, srcPrefix *string, dstPrefix *string) *string {
	var finalKey string
	var dstSuffix string

	if strings.HasSuffix(*dstPrefix, "/") {
		if strings.EqualFold(*srcPrefix, *srcKey) {
			srcArray := strings.Split(*srcKey, "/")
			dstSuffix = srcArray[len(srcArray)-1]
		} else {
			dstSuffix = SubString(*srcKey, *srcPrefix)
		}
		finalKey = *dstPrefix + dstSuffix
		return &finalKey
	} else {
		return dstPrefix
	}
}

func ReadDeltaLakeManifestFile(srcClient *s3.S3, srcBucket *string, srcPrefix *string) (string, error) {
	manifestSuffix := *srcPrefix + "/_symlink_format_manifest/manifest"
	result, err := srcClient.GetObject(&s3.GetObjectInput{
		Bucket: srcBucket,
		Key:    &manifestSuffix,
	})

	if err != nil {
		return "", fmt.Errorf("error getting Deltalake manifest file: %s", err)
	}

	body, readBodyErr := ioutil.ReadAll(result.Body)
	if readBodyErr != nil {
		return "", fmt.Errorf("error reading Deltalake manifest file body: %s", readBodyErr)
	}
	return string(body), nil
}

func ExitError(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func Retry(attempts int, sleep time.Duration,
	f func(copyMetaInfo *CopyMetaInfo, multiPartUploadThreshold *int64) error,
	copyMetaIno *CopyMetaInfo, multiPartUploadThreshold *int64) (err error) {
	for i := 0; i < attempts; i++ {
		if i > 0 {
			log.Println("retrying after error:", err)
			time.Sleep(sleep)
		}
		err = f(copyMetaIno, multiPartUploadThreshold)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

func CalculatePartNumber(objectSize *int64, multiPartUploadThreshold *int64) int64 {
	if *objectSize > 0 {
		return int64(math.Ceil(float64(*objectSize) / float64(*multiPartUploadThreshold)))
	} else {
		return 1
	}
}

func base64Sum(content []byte) string {
	sum := md5.Sum(content)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func SubString(s1 string, s2 string) string {
	if idx := strings.Index(s1, s2); idx != -1 {
		return s1[idx+len(s2):]
	}
	return s1
}

// completedParts is a wrapper to make parts sortable by their part number,
// since S3 required this list to be sent in sorted order.
type completedParts []*s3.CompletedPart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }
