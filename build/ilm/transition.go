/*
*
*  Mint, (C) 2021 Minio, Inc.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software

*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
 */

package main

import (
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Tests ilm transition rules
func testTransitionRules() {
	lConfigFuture := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("transitiondateinfuture"),
				Status: aws.String("Enabled"),
				Transitions: []*s3.Transition{
					{
						Date:         aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)),
						StorageClass: aws.String(tierName),
					},
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	lConfigPast := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("transitiondateinpast"),
				Status: aws.String("Enabled"),
				Transitions: []*s3.Transition{
					{
						Date:         aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)),
						StorageClass: aws.String(tierName),
					},
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	lConfigPastPrefix := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("transitiondateinpast"),
				Status: aws.String("Enabled"),
				Transitions: []*s3.Transition{
					{
						Date:         aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)),
						StorageClass: aws.String(tierName),
					},
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String("prefix"),
				},
			},
		},
	}

	testCases := []struct {
		lConfig       *s3.BucketLifecycleConfiguration
		object        string
		expTransition bool
	}{
		// testTransitionRules case - 1.
		// Transition date in future, object not transitioned
		{
			lConfig:       lConfigFuture,
			object:        "object",
			expTransition: false,
		},
		// testTransitionRules case - 2.
		// Transition date in past, rule without prefix filter
		{
			lConfig:       lConfigPast,
			object:        "object",
			expTransition: true,
		},
		// testTransitionRules case - 3.
		// Transition date in past, rule with prefix filter does not match
		{
			lConfig:       lConfigPastPrefix,
			object:        "object",
			expTransition: false,
		},
		// testTransitionRules case - 4.
		// Transition date in past, rule with prefix filter matches
		{
			lConfig:       lConfigPastPrefix,
			object:        "prefix/object",
			expTransition: true,
		},
	}

	for i, testCase := range testCases {
		execTestTransitionRules(i, testCase)
	}

}

func execTestTransitionRules(i int, testCase struct {
	lConfig       *s3.BucketLifecycleConfiguration
	object        string
	expTransition bool
}) {
	// initialize logging params
	startTime := time.Now()
	function := "testTransitionRules"
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "ilm-test-")
	args := map[string]interface{}{
		"testCase":      i,
		"bucketName":    bucketName,
		"objectName":    testCase.object,
		"expTransition": testCase.expTransition,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanBucket(bucketName, function, args, startTime)

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: testCase.lConfig,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("my content 1")),
		Bucket: aws.String(bucketName),
		Key:    aws.String(testCase.object),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT expected to succeed but failed", err).Error()
		return
	}

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testCase.object),
	}

	// wait some time before getting object the first time
	// transition is an async process
	time.Sleep(1 * time.Second)

	// get with 3 retries
	var result *s3.GetObjectOutput
	for i := 0; i < 3; i++ {
		result, err = s3Client.GetObject(getInput)
		if err != nil {
			failureLog(function, args, startTime, "", "GET expected to succeed but failed", err).Error()
			return
		}

		if testCase.expTransition && result.StorageClass != nil && *(result.StorageClass) == tierName {
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	if testCase.expTransition && (result.StorageClass == nil || *result.StorageClass != tierName) {
		failureLog(function, args, startTime, "", "Expected object to be transitioned.", nil).Error()
		return
	}

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "Expected to return data but failed", err).Error()
		return
	}
	_ = result.Body.Close()

	if string(body) != "my content 1" {
		failureLog(function, args, startTime, "", "Unexpected body content", err).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}
