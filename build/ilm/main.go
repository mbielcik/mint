//  Mint, (C) 2021 Minio, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// S3 client for testing
var s3Client *s3.S3

func cleanup(s3Client *s3.S3, bucket string, object string, function string,
	args map[string]interface{}, startTime time.Time, deleteBucket bool) {

	// Deleting the object, just in case it was created. Will not check for errors.
	_, _ = s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	if deleteBucket {
		_, err := s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			failureLog(function, args, startTime, "", "ILM Test DeleteBucket Failed", err).Fatal()
			return
		}
	}
}

func isPutLifecycleConfigurationImplemented(s3Client *s3.S3) bool {
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "ilm-test-")
	startTime := time.Now()
	function := "isPutLifecycleConfigurationImplemented"
	args := map[string]interface{}{
		"bucketName": bucket,
	}
	defer cleanup(s3Client, bucket, "", function, args, startTime, true)

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "ILM Test CreateBucket Failed", err).Fatal()
		return false
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					ID:     aws.String("checkilmimplemented"),
					Status: aws.String("Enabled"),
					Expiration: &s3.LifecycleExpiration{
						Date: aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String("/"),
					},
				},
			},
		},
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotImplemented" {
				return false
			}
		}
	}

	return true
}

func main() {
	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Fatal level
	log.SetLevel(log.InfoLevel)

	endpoint := os.Getenv("SERVER_ENDPOINT")
	accessKey := os.Getenv("ACCESS_KEY")
	secretKey := os.Getenv("SECRET_KEY")
	secure := os.Getenv("ENABLE_HTTPS")
	sdkEndpoint := "http://" + endpoint
	if secure == "1" {
		sdkEndpoint = "https://" + endpoint
	}

	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	s3Config := &aws.Config{
		Credentials:      creds,
		Endpoint:         aws.String(sdkEndpoint),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		args := map[string]interface{}{
			"sdkEndpoint": sdkEndpoint,
		}
		failureLog("main", args, time.Now(), "", "Failed to create aws session to connect to minio server.", err).Fatal()
		return
	}

	// Create an S3 service object in the default region.
	s3Client = s3.New(newSession, s3Config)

	putLcImpl := isPutLifecycleConfigurationImplemented(s3Client)
	if !putLcImpl {
		ignoreLog("main", map[string]interface{}{}, time.Now(), "PutLifecycleConfiguration is not implemented. Skipping all ILM tests.").Info()
		return
	}

	testDeletionRules()
}
