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
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// S3 client for testing
var serverEnvCfg = loadEnvConfig()
var s3Client *s3.S3
var tierName string

func main() {
	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Error level
	log.SetLevel(log.InfoLevel)

	var err error
	s3Client, err = createS3Client(serverEnvCfg)
	if err != nil {
		failureLog("main", map[string]interface{}{}, time.Now(), "", "Failed to create aws session to connect to minio server.", err).Error()
		return
	}

	putLcImpl := isPutLifecycleConfigurationImplemented()
	if !putLcImpl {
		ignoreLog("main", map[string]interface{}{}, time.Now(), "PutLifecycleConfiguration is not implemented. Skipping all ILM tests.").Info()
		return
	}

	testExpiry()

	if serverEnvCfg.remoteTierName == "" {
		ignoreLog("main", map[string]interface{}{}, time.Now(), "No remote tier name given. Therefore ILM-Tiering tests will be skipped. "+
			"Provide env 'REMOTE_TIER_NAME' with name of the configured remote tier to enable ILM-Tiering tests.").Info()
		return
	}
	tierName = serverEnvCfg.remoteTierName

	testTransition()
	testExpireTransitioned()
	testRestore()
	testRestoreMultipart()

	cleanupWg.Wait()
}

var cleanupWg sync.WaitGroup

func addCleanBucket(bucket string, function string, args map[string]interface{}, startTime time.Time) {
	cleanupWg.Add(1)

	go func() {
		defer cleanupWg.Done()
		cleanupBucket(bucket, function, args, startTime)
	}()
}

func cleanupBucket(bucket string, function string, args map[string]interface{}, startTime time.Time) {
	start := time.Now()

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}

	var err error
	for time.Since(start) < 3*time.Minute {
		err = s3Client.ListObjectsPages(input,
			func(page *s3.ListObjectsOutput, lastPage bool) bool {
				for _, o := range page.Contents {
					input := &s3.DeleteObjectInput{
						Bucket: &bucket,
						Key:    o.Key,
					}
					_, err := s3Client.DeleteObject(input)
					if err != nil {
						return true
					}
				}
				return true
			})

		_, err = s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}

	failureLog(function, args, startTime, "", fmt.Sprintf("Unable to cleanup bucket '%s' after ILM tests", bucket), err).Error()
	return
}

func isPutLifecycleConfigurationImplemented() bool {
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "ilm-test-")
	startTime := time.Now()
	function := "isPutLifecycleConfigurationImplemented"
	args := map[string]interface{}{
		"bucketName": bucket,
	}
	defer addCleanBucket(bucket, function, args, startTime)

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
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
						Prefix: aws.String(""),
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
