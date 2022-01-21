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
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	minio "github.com/minio/minio-go/v7"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Tests ilm expiration rules with versioned buckets

func testExpireCurrentVersion() {
	lConfigPast := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirydateinpast"),
				Status: aws.String("Enabled"),
				Expiration: &s3.LifecycleExpiration{
					Date: aws.Time(time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2)),
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	// initialize logging params
	startTime := time.Now()
	function := "testExpireCurrentVersion"
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "ilm-test-")
	objectName := "object"
	contents := []string{"my content 1", "my content 2"}
	args := map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanupBucket(bucketName, function, args, startTime, true)

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	}

	_, err = s3Client.PutBucketVersioning(putVersioningInput)
	if err != nil {
		failureLog(function, args, startTime, "", "Put VersioningConfiguration failed", err).Error()
		return
	}

	putOutputs := make([]*s3.PutObjectOutput, 0, len(contents))
	for i, content := range contents {
		putInput := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(strings.NewReader(content)),
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectName),
		}
		putOutput, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT (%d) expected to succeed but failed", i), err).Error()
			return
		}

		putOutputs = append(putOutputs, putOutput)
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigPast,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	getInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	// trigger lifecycle and wait
	_, _ = s3Client.GetObject(getInput)
	time.Sleep(time.Second)

	_, err = s3Client.GetObject(getInput)
	if err == nil {
		failureLog(function, args, startTime, "", "Expected current object version to be deleted", nil).Error()
		return
	}
	aerr, ok := err.(awserr.Error)
	if !ok {
		failureLog(function, args, startTime, "", "Unexpected non aws error on GetObject", err).Error()
		return
	}
	if aerr.Code() != "NoSuchKey" {
		failureLog(function, args, startTime, "", "Unexpected aws error on GetObject", err).Error()
		return
	}

	// Get the older versions, make sure it is preserved
	for i, output := range putOutputs {
		getVersionInput := &s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(objectName),
			VersionId: aws.String(*output.VersionId),
		}

		getVersionResult, err := s3Client.GetObject(getVersionInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject (%d) expected to succeed but failed.", i), err).Error()
			return
		}

		body, err := ioutil.ReadAll(getVersionResult.Body)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject (%d) expected to return data but failed.", i), err).Error()
			return
		}
		_ = getVersionResult.Body.Close()

		if string(body) != contents[i] {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject (%d) unexpected body content", i), nil).Error()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

func testExpireNonCurrentVersions() {
	lConfigPast := &s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID:     aws.String("expirydeletemarkers"),
				Status: aws.String("Enabled"),
				NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
					NoncurrentDays: aws.Int64(2),
				},
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
			},
		},
	}

	// initialize logging params
	startTime := time.Now()
	function := "testExpireNonCurrentVersions"
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "ilm-test-")
	objectName := "object"
	objects := []struct {
		content     string
		isCurrent   bool
		expDeletion bool
		modTime     time.Time
	}{
		{
			content:     "my content 1",
			isCurrent:   false,
			expDeletion: true,
			modTime:     time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -5),
		},
		{
			content:     "my content 2",
			isCurrent:   false,
			expDeletion: true,
			modTime:     time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -4),
		},
		{
			content:     "my content 3",
			isCurrent:   false,
			expDeletion: false,
			modTime:     time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -3),
		},
		{
			content:     "my content 4",
			isCurrent:   true,
			expDeletion: false,
			modTime:     time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -2),
		},
	}
	args := map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket Failed", err).Error()
		return
	}
	defer addCleanupBucket(bucketName, function, args, startTime, true)

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	}
	_, err = s3Client.PutBucketVersioning(putVersioningInput)
	if err != nil {
		failureLog(function, args, startTime, "", "Put VersioningConfiguration failed", err).Error()
		return
	}

	putResults := make([]minio.UploadInfo, 0, len(objects))
	for i, object := range objects {
		putResult, err := minioClient.PutObject(
			context.Background(),
			bucketName,
			objectName,
			strings.NewReader(object.content),
			int64(len(object.content)),
			minio.PutObjectOptions{
				Internal: minio.AdvancedPutOptions{
					SourceMTime: object.modTime,
				},
			},
		)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT (%d) expected to succeed but failed", i), err).Error()
			return
		}

		putResults = append(putResults, putResult)
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigPast,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	for i, object := range objects {
		getVersionedInput := &s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(objectName),
			VersionId: aws.String(putResults[i].VersionID),
		}

		// trigger lifecycle and wait
		_, _ = s3Client.GetObject(getVersionedInput)
		time.Sleep(time.Second)

		_, err = s3Client.GetObject(getVersionedInput)
		objectFound := true
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if !ok {
				failureLog(function, args, startTime, "", fmt.Sprintf("Unexpected non aws error on GetObject (%d)", i), err).Error()
				return
			}
			if aerr.Code() != "NoSuchVersion" {
				failureLog(function, args, startTime, "", fmt.Sprintf("Unexpected aws error on GetObject (%d)", i), err).Error()
				return
			}
			objectFound = false
		}

		if !object.expDeletion && !objectFound {
			failureLog(function, args, startTime, "", fmt.Sprintf("Expected object version (%d) to be found.", i), err).Error()
			return
		}

		if object.expDeletion && objectFound {
			failureLog(function, args, startTime, "", fmt.Sprintf("Expected object version (%d) to be deleted", i), nil).Error()
			return
		}
	}

	getVerInput := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}

	listVerResult, err := s3Client.ListObjectVersions(getVerInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("ListObjectVersions expected to succeed but got %v", err), err).Error()
		return
	}

	if len(listVerResult.DeleteMarkers) != 0 {
		failureLog(function, args, startTime, "", "Expected ListObjectVersions to no DeleteMarkers.", nil).Error()
		return
	}

	currentVerIdx := 0
	expVersionsIdx := make([]int, 0)
	for i, object := range objects {
		if !object.expDeletion {
			expVersionsIdx = append(expVersionsIdx, i)
		}
		if object.isCurrent {
			currentVerIdx = i
		}
	}

	if len(listVerResult.Versions) != len(expVersionsIdx) {
		failureLog(function, args, startTime, "", fmt.Sprintf("Expected ListObjectVersions to return (%d) versions.", len(expVersionsIdx)), nil).Error()
		return
	}

	currentVer := listVerResult.Versions[0].VersionId
	if currentVer == nil {
		failureLog(function, args, startTime, "", "Expected current versionId to be not empty.", nil).Error()
		return
	}

	if putResults[currentVerIdx].VersionID != *currentVer {
		failureLog(function, args, startTime, "", fmt.Sprintf("Expected current version to be %s.", putResults[currentVerIdx].VersionID), nil).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}

// like testExpireNonCurrentVersions, but delete the object after
// the two put calls, then get all 3 versions to trigger the lc rules
// nothing, not even the delete marker should exist anymore
// maybe also check with listobjectversions
// func testExpireDeleteMarkers() {
// }
