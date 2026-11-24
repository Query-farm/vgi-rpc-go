// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgis3

import (
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestR2 runs every storage role against Cloudflare R2's S3 API (direct-
// storage contract v1, Addendum B) when R2_TEST_ENDPOINT, R2_TEST_BUCKET,
// R2_TEST_PREFIX, R2_TEST_REGION and the R2_TEST_STATIC_* / R2_TEST_TEMP_*
// credentials are set: with static keys and with session-token credentials
// (capped at R2_TEST_TEMP_EXPIRES_EPOCH), with checksum sha256 (a tampered
// upload must be refused) and none. Everything stays under R2_TEST_PREFIX.
func TestR2(t *testing.T) {
	endpoint := os.Getenv("R2_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("R2_TEST_ENDPOINT not set")
	}
	bucket, region := os.Getenv("R2_TEST_BUCKET"), os.Getenv("R2_TEST_REGION")
	prefix := os.Getenv("R2_TEST_PREFIX") + "vgi-rpc-go/" + strconv.FormatInt(time.Now().Unix(), 36) + "/"
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	expires, _ := strconv.ParseInt(os.Getenv("R2_TEST_TEMP_EXPIRES_EPOCH"), 10, 64)
	creds := map[string]S3Config{
		"static": {AccessKeyID: os.Getenv("R2_TEST_STATIC_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("R2_TEST_STATIC_SECRET_ACCESS_KEY")},
		"session": {AccessKeyID: os.Getenv("R2_TEST_TEMP_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("R2_TEST_TEMP_SECRET_ACCESS_KEY"),
			SessionToken: os.Getenv("R2_TEST_TEMP_SESSION_TOKEN"), SessionExpires: time.Unix(expires, 0)},
	}
	for name, c := range creds {
		for _, checksum := range []string{ChecksumSHA256, ChecksumNone} {
			t.Run(name+"/"+checksum, func(t *testing.T) {
				c.EndpointURL, c.Region, c.Prefix, c.ExactPrefix, c.Checksum = endpoint, region, prefix, true, checksum
				storage, err := NewS3Storage(bucket, c)
				if err != nil {
					t.Fatal(err)
				}
				exerciseAllRoles(t, minioEnv{endpoint: endpoint, bucket: bucket}, storage)
				if name == "session" && checksum == ChecksumSHA256 {
					grant, err := storage.GenerateUploadURL(1, sha256Hex([]byte("x")))
					if err != nil {
						t.Fatal(err)
					}
					q, _ := url.Parse(grant.UploadURL)
					ttl, _ := strconv.Atoi(q.Query().Get("X-Amz-Expires"))
					if q.Query().Get("X-Amz-Security-Token") == "" || time.Now().Add(time.Duration(ttl)*time.Second).After(time.Unix(expires, 0)) {
						t.Errorf("session grant: token present=%v, X-Amz-Expires=%d outlives the session", q.Query().Get("X-Amz-Security-Token") != "", ttl)
					}
				}
			})
		}
	}
}
