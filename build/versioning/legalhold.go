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
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// Creates a testobject
func createTestObject(size int64, name string) {
	fd, err := os.Create(name)
	if err != nil {
		log.Fatal("Failed to create testfile")
	}
	_, err = fd.Seek(size-1, 0)
	if err != nil {
		log.Fatal("Failed to seek")
	}
	_, err = fd.Write([]byte{0})
	if err != nil {
		log.Fatal("Write failed")
	}
	err = fd.Close()
	if err != nil {
		log.Fatal("Failed to close file")
	}
}

// Test locking for different versions
func testLockingLegalhold() {
	startTime := time.Now()
	function := "testLockingLegalhold"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		legalhold        string
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{legalhold: "ON"},
		{legalhold: "OFF"},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:                      aws.ReadSeekCloser(strings.NewReader("content")),
			Bucket:                    aws.String(bucket),
			Key:                       aws.String(object),
			ObjectLockLegalHoldStatus: aws.String(uploads[i].legalhold),
		}
		output, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(deleteInput)
		if err == nil && uploads[i].legalhold == "ON" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].legalhold == "OFF" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.GetObjectLegalHoldInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err := s3Client.GetObjectLegalHold(input)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			LegalHold: &s3.ObjectLockLegalHold{Status: aws.String("OFF")},
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err := s3Client.PutObjectLegalHold(input)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold failed with %v", err), err).Fatal()
			return
		}
	}

	// Error cases

	// object-handlers.go > GetObjectLegalHoldHandler > getObjectInfo
	for i := range uploads {
		if uploads[i].legalhold == "" || uploads[i].legalhold == "OFF" {
			input := &s3.GetObjectLegalHoldInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String(object),
				VersionId: aws.String(uploads[i].versionId),
			}
			// legalhold = "off" => The specified version does not exist.
			// legalhold = ""    => The specified method is not allowed against this resource.
			_, err := s3Client.GetObjectLegalHold(input)
			if err == nil {
				failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
				return
			}
		}
	}

	// Second client
	creds := credentials.NewStaticCredentials("test", "test", "")
	newSession, err := session.NewSession()
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("NewSession expected to succeed but got %v", err), err).Fatal()
		return
	}
	s3Config := s3Client.Config
	s3Config.Credentials = creds
	s3ClientTest := s3.New(newSession, &s3Config)

	// Check with a second client: object-handlers.go > GetObjectLegalHoldHandler > checkRequestAuthType
	input := &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	// The Access Key Id you provided does not exist in our records.
	_, err = s3ClientTest.GetObjectLegalHold(input)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	// object-handlers.go > GetObjectLegalHoldHandler > globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled
	bucketWithoutLock := bucket + "-without-lock"
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucketWithoutLock),
		ObjectLockEnabledForBucket: aws.Bool(false),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucketWithoutLock, function, args, startTime)

	input = &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketWithoutLock),
		Key:    aws.String(object),
	}
	// Bucket is missing ObjectLockConfiguration
	_, err = s3Client.GetObjectLegalHold(input)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	// Check with a second client: object-handlers.go > PutObjectLegalHoldHandler > checkRequestAuthType
	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		// The Access Key Id you provided does not exist in our records.
		_, err := s3ClientTest.PutObjectLegalHold(input)
		if err == nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// object-handlers.go > PutObjectLegalHoldHandler > globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled
	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucketWithoutLock),
			Key:    aws.String(object),
		}
		// Bucket is missing ObjectLockConfiguration
		_, err := s3Client.PutObjectLegalHold(input)
		if err == nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// object-handlers.go > PutObjectLegalHoldHandler > objectlock.ParseObjectLegalHold
	putInput := &s3.PutObjectInput{
		Body:                      aws.ReadSeekCloser(strings.NewReader("content")),
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(object),
		ObjectLockLegalHoldStatus: aws.String("test"),
	}
	output, err := s3Client.PutObject(putInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}
	uploads[0].versionId = *output.VersionId

	polhInput := &s3.PutObjectLegalHoldInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(uploads[0].versionId),
	}
	// We encountered an internal error, please try again.: cause(EOF)
	_, err = s3Client.PutObjectLegalHold(polhInput)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PutObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}

// Test locking for different versions (multipart)
func testLockingLegalholdMultipart() {
	startTime := time.Now()
	function := "testLockingLegalholdMultipart"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testobject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	fileSize := 30 * 1024 * 1024
	createTestObject(int64(fileSize), object)

	f, err := os.Open(object)
	if err != nil {
		failureLog(function, args, startTime, "", "Open testobject failed", err).Fatal()
		return
	}
	defer f.Close()
	defer os.Remove(object)

	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		legalhold        string
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{legalhold: "ON"},
		{legalhold: "OFF"},
	}

	partSize := 5 * 1024 * 1024 // Set part size to 5 MB (minimum size for a part)

	// Upload versions and save their version IDs
	for i := range uploads {
		multipartUpload, err := s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String(object),
			ObjectLockLegalHoldStatus: aws.String(uploads[i].legalhold),
		})
		if err != nil {
			failureLog(function, args, startTime, "", "CreateMultipartupload API failed", err).Fatal()
			return
		}

		filePart := make([]byte, partSize)
		partCount := fileSize / partSize
		parts := make([]*string, partCount)
		for j := 0; j < partCount; j++ {
			_, err := f.ReadAt(filePart, int64(partSize*j))
			if err != nil {
				failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
				return
			}
			r := bytes.NewReader(filePart)

			result, errUpload := s3Client.UploadPart(&s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(object),
				UploadId:   multipartUpload.UploadId,
				PartNumber: aws.Int64(int64(j + 1)),
				Body:       aws.ReadSeekCloser(r),
			})
			if errUpload != nil {
				_, _ = s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(object),
					UploadId: multipartUpload.UploadId,
				})
				failureLog(function, args, startTime, "", "UploadPart API failed for", errUpload).Fatal()
				return
			}
			parts[j] = result.ETag
		}

		completedParts := make([]*s3.CompletedPart, len(parts))
		for i, part := range parts {
			completedParts[i] = &s3.CompletedPart{
				ETag:       part,
				PartNumber: aws.Int64(int64(i + 1)),
			}
		}

		output, err := s3Client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completedParts},
			UploadId: multipartUpload.UploadId,
		})
		if err != nil {
			failureLog(function, args, startTime, "", "CompleteMultipartUpload is expected to succeed but failed", errors.New("expected nil")).Fatal()
			return
		}

		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(deleteInput)
		if err == nil && uploads[i].legalhold == "ON" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].legalhold == "OFF" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.GetObjectLegalHoldInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err := s3Client.GetObjectLegalHold(input)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			LegalHold: &s3.ObjectLockLegalHold{Status: aws.String("OFF")},
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err := s3Client.PutObjectLegalHold(input)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold failed with %v", err), err).Fatal()
			return
		}
	}

	// Error cases

	// object-handlers.go > GetObjectLegalHoldHandler > getObjectInfo
	for i := range uploads {
		if uploads[i].legalhold == "" || uploads[i].legalhold == "OFF" {
			input := &s3.GetObjectLegalHoldInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String(object),
				VersionId: aws.String(uploads[i].versionId),
			}
			// legalhold = "off" => The specified version does not exist.
			// legalhold = ""    => The specified method is not allowed against this resource.
			_, err := s3Client.GetObjectLegalHold(input)
			if err == nil {
				failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
				return
			}
		}
	}

	// Second client
	creds := credentials.NewStaticCredentials("test", "test", "")
	newSession, err := session.NewSession()
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("NewSession expected to succeed but got %v", err), err).Fatal()
		return
	}
	s3Config := s3Client.Config
	s3Config.Credentials = creds
	s3ClientTest := s3.New(newSession, &s3Config)

	// Check with a second client: object-handlers.go > GetObjectLegalHoldHandler > checkRequestAuthType
	input := &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	// The Access Key Id you provided does not exist in our records.
	_, err = s3ClientTest.GetObjectLegalHold(input)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	// object-handlers.go > GetObjectLegalHoldHandler > globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled
	bucketWithoutLock := bucket + "-without-lock"
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucketWithoutLock),
		ObjectLockEnabledForBucket: aws.Bool(false),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucketWithoutLock, function, args, startTime)

	input = &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketWithoutLock),
		Key:    aws.String(object),
	}
	// Bucket is missing ObjectLockConfiguration
	_, err = s3Client.GetObjectLegalHold(input)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	// Check with a second client: object-handlers.go > PutObjectLegalHoldHandler > checkRequestAuthType
	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		// The Access Key Id you provided does not exist in our records.
		_, err := s3ClientTest.PutObjectLegalHold(input)
		if err == nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// object-handlers.go > PutObjectLegalHoldHandler > globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled
	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucketWithoutLock),
			Key:    aws.String(object),
		}
		// Bucket is missing ObjectLockConfiguration
		_, err := s3Client.PutObjectLegalHold(input)
		if err == nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// object-handlers.go > PutObjectLegalHoldHandler > objectlock.ParseObjectLegalHold
	putInput := &s3.PutObjectInput{
		Body:                      aws.ReadSeekCloser(strings.NewReader("content")),
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(object),
		ObjectLockLegalHoldStatus: aws.String("test"),
	}
	output, err := s3Client.PutObject(putInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}
	uploads[0].versionId = *output.VersionId

	polhInput := &s3.PutObjectLegalHoldInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(uploads[0].versionId),
	}
	// We encountered an internal error, please try again.: cause(EOF)
	_, err = s3Client.PutObjectLegalHold(polhInput)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PutObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	// Omit a part when uploading
	multipartUpload, err := s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(object),
		ObjectLockLegalHoldStatus: aws.String(uploads[0].legalhold),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateMultipartupload API failed", err).Fatal()
		return
	}

	filePart := make([]byte, partSize)
	partCount := fileSize / partSize
	parts := make([]*string, partCount)
	for j := 0; j < partCount-1; j++ {
		_, err := f.ReadAt(filePart, int64(partSize*j))
		if err != nil {
			failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
			return
		}
		r := bytes.NewReader(filePart)

		result, errUpload := s3Client.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(object),
			UploadId:   multipartUpload.UploadId,
			PartNumber: aws.Int64(int64(j + 1)),
			Body:       aws.ReadSeekCloser(r),
		})
		if errUpload != nil {
			_, _ = s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(object),
				UploadId: multipartUpload.UploadId,
			})
			failureLog(function, args, startTime, "", "UploadPart API failed for", errUpload).Fatal()
			return
		}
		parts[j] = result.ETag
	}

	completedParts := make([]*s3.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = &s3.CompletedPart{
			ETag:       part,
			PartNumber: aws.Int64(int64(i + 1)),
		}
	}

	_, err = s3Client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts},
		UploadId: multipartUpload.UploadId,
	})
	// One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.
	if err == nil {
		failureLog(function, args, startTime, "", "CompleteMultipartUpload is expected to fail but succeeded", err).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}
