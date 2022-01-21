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
	objectContent1 := "my content 1"
	objectContent2 := "my content 2"
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

	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(objectContent1)),
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}
	putOutput1, err := s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT expected to succeed but failed", err).Error()
		return
	}

	putInput2 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(objectContent2)),
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}
	putOutput2, err := s3Client.PutObject(putInput2)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT expected to succeed but failed", err).Error()
		return
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

	foundCurrent := false
	for i := 0; i < 3; i++ {
		_, err = s3Client.GetObject(getInput)
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if !ok {
				failureLog(function, args, startTime, "", "Unexpected non aws error on GetObject", err).Error()
				return
			}
			if aerr.Code() == "NotFound" {
				foundCurrent = false
				break
			}
		}

		foundCurrent = true
		time.Sleep(300 * time.Millisecond)
	}

	if foundCurrent {
		failureLog(function, args, startTime, "", "Expected current object version to be deleted", nil).Error()
		return
	}

	// Get the older version, make sure it is preserved
	getInput1 := &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		VersionId: aws.String(*putOutput1.VersionId),
	}

	getVer1Result, err := s3Client.GetObject(getInput1)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObject ver1 expected to succeed but failed with %v", err), err).Error()
		return
	}

	bodyVer1, err := ioutil.ReadAll(getVer1Result.Body)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObject ver1 expected to return data but failed with %v", err), err).Error()
		return
	}
	getVer1Result.Body.Close()

	if string(bodyVer1) != objectContent1 {
		failureLog(function, args, startTime, "", "GetObject ver1 unexpected body content", nil).Error()
		return
	}

	getInput2 := &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		VersionId: aws.String(*putOutput2.VersionId),
	}

	getVer2Result, err := s3Client.GetObject(getInput2)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObject ver2 expected to succeed but failed with %v", err), err).Error()
		return
	}

	bodyVer2, err := ioutil.ReadAll(getVer2Result.Body)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObject ver2 expected to return data but failed with %v", err), err).Error()
		return
	}
	getVer2Result.Body.Close()

	if string(bodyVer2) != objectContent2 {
		failureLog(function, args, startTime, "", "GetObject ver2 unexpected body content", nil).Error()
		return
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
					NoncurrentDays: aws.Int64(1),
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
	objectContent1 := "my content 1"
	objectContent2 := "my content 2"
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

	putResult1, err := minioClient.PutObject(
		context.Background(),
		bucketName,
		objectName,
		strings.NewReader(objectContent1),
		int64(len(objectContent1)),
		minio.PutObjectOptions{
			Internal: minio.AdvancedPutOptions{
				SourceMTime: time.Now().Add(-3 * 24 * time.Hour),
			},
		},
	)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT ver1 expected to succeed but failed", err).Error()
		return
	}

	putResult2, err := minioClient.PutObject(
		context.Background(),
		bucketName,
		objectName,
		strings.NewReader(objectContent2),
		int64(len(objectContent2)),
		minio.PutObjectOptions{
			Internal: minio.AdvancedPutOptions{
				SourceMTime: time.Now().Add(-2 * 24 * time.Hour),
			},
		},
	)
	if err != nil {
		failureLog(function, args, startTime, "", "PUT ver2 expected to succeed but failed", err).Error()
		return
	}

	_, err = s3Client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: lConfigPast,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Put LifecycleConfiguration failed", err).Error()
		return
	}

	getInputNonCurrent := &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		VersionId: aws.String(putResult1.VersionID),
	}

	foundNonCurrent := false
	for i := 0; i < 3; i++ {
		_, err = s3Client.GetObject(getInputNonCurrent)
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if !ok {
				failureLog(function, args, startTime, "", "Unexpected non aws error on GetObject", err).Error()
				return
			}
			if aerr.Code() == "NotFound" {
				foundNonCurrent = false
				break
			}

			failureLog(function, args, startTime, "", "Unexpected aws error on GetObject", err).Error()
			return
		}

		foundNonCurrent = true
		time.Sleep(300 * time.Millisecond)
	}

	if foundNonCurrent {
		failureLog(function, args, startTime, "", "Expected non current object version to be deleted", nil).Error()
		return
	}

	getInputCurrent := &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		VersionId: aws.String(putResult2.VersionID),
	}

	foundCurrent := false
	for i := 0; i < 3; i++ {
		_, err = s3Client.GetObject(getInputCurrent)
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if !ok {
				failureLog(function, args, startTime, "", "Unexpected non aws error on GetObject", err).Error()
				return
			}
			if aerr.Code() == "NotFound" {
				foundNonCurrent = false
				time.Sleep(300 * time.Millisecond)
				continue
			}

			failureLog(function, args, startTime, "", "Unexpected aws error on GetObject", err).Error()
			return
		}

		foundCurrent = true
		time.Sleep(300 * time.Millisecond)
	}

	if !foundCurrent {
		failureLog(function, args, startTime, "", "Expected non current object version to be deleted", nil).Error()
		return
	}

	getVerInput := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}

	listVerResult, err := s3Client.ListObjectVersions(getVerInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("ListObjectVersions expected to succeed but got %v", err), err).Error()
		return
	}

	if len(listVerResult.DeleteMarkers) != 0 || len(listVerResult.Versions) != 1 {
		failureLog(function, args, startTime, "", "Expected ListObjectVersions to return only 1 version and no DeleteMarkers.",
			nil).Error()
		return
	}

	if listVerResult.Versions[0].VersionId == nil || putResult2.VersionID != *(listVerResult.Versions[0].VersionId) {
		failureLog(function, args, startTime, "", "Expected version does not match.",
			nil).Error()
		return
	}

	successLogger(function, args, startTime).Info()
}

// wie oben aber objekt löschen und dann die letzten 3 versionen inkl. deletemarker abfragen
// es sollte nichts mehr übrig sein. evtl mit listobjectversions
// testExpireDeleteMarkers
