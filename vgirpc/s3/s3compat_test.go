// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgis3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// The S3-compatibility tests run against a real S3-compatible server when
// VGI_S3_TEST_ENDPOINT (e.g. http://127.0.0.1:9000 or an https:// endpoint;
// for a self-signed certificate also set AWS_CA_BUNDLE and SSL_CERT_FILE),
// VGI_S3_TEST_BUCKET, VGI_S3_TEST_ACCESS_KEY and VGI_S3_TEST_SECRET_KEY are
// set. The bucket is created if missing. CI runs them against RustFS (and they pass against MinIO) over
// plain http (path style) and over TLS.

type minioEnv struct {
	endpoint, bucket, access, secret string
}

func s3compat(t *testing.T) minioEnv {
	t.Helper()
	e := minioEnv{
		endpoint: os.Getenv("VGI_S3_TEST_ENDPOINT"),
		bucket:   os.Getenv("VGI_S3_TEST_BUCKET"),
		access:   os.Getenv("VGI_S3_TEST_ACCESS_KEY"),
		secret:   os.Getenv("VGI_S3_TEST_SECRET_KEY"),
	}
	if e.endpoint == "" {
		t.Skip("VGI_S3_TEST_ENDPOINT not set")
	}
	// No region and no profile from the host: the backend must default.
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(e.access, e.secret, "")))
	if err != nil {
		t.Fatal(err)
	}
	c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(e.endpoint); o.UsePathStyle = true })
	_, _ = c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(e.bucket)})
	return e
}

// assumeRole asks the server's STS endpoint (MinIO implements AssumeRole)
// for temporary credentials.
func (e minioEnv) assumeRole(t *testing.T, seconds int32) aws.Credentials {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(e.access, e.secret, "")))
	if err != nil {
		t.Fatal(err)
	}
	client := sts.NewFromConfig(cfg, func(o *sts.Options) { o.BaseEndpoint = aws.String(e.endpoint) })
	out, err := client.AssumeRole(context.Background(), &sts.AssumeRoleInput{
		RoleArn:         aws.String("arn:xxx:xxx:xxx:xxxx"),
		RoleSessionName: aws.String("vgi-rpc-go-test"),
		DurationSeconds: aws.Int32(seconds),
	})
	if err != nil {
		t.Fatalf("STS AssumeRole: %v", err)
	}
	if out.Credentials == nil || aws.ToString(out.Credentials.SessionToken) == "" {
		t.Fatal("STS AssumeRole returned no session token")
	}
	return aws.Credentials{
		AccessKeyID:     aws.ToString(out.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(out.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(out.Credentials.SessionToken),
		CanExpire:       true,
		Expires:         aws.ToTime(out.Credentials.Expiration),
	}
}

// exerciseAllRoles runs every storage role against the server: object store
// round trip and missing key, result upload + presigned GET, and an upload
// grant that storage enforces (wrong bytes of the right size refused, the
// right bytes accepted and fetched by object id).
func exerciseAllRoles(t *testing.T, e minioEnv, storage *S3Storage) {
	t.Helper()
	payload := bytes.Repeat([]byte("minio"), 1<<18)

	key, err := storage.PutObject(payload)
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	got, err := storage.Get(key, 0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get: err=%v equal=%v", err, bytes.Equal(got, payload))
	}
	if _, err := storage.Get(key+"-missing", 0); err == nil || !strings.Contains(err.Error(), "gone") {
		t.Fatalf("missing key: %v", err)
	}

	presigned, err := storage.Upload(payload, nil, "")
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	checkHost(t, presigned, storage.publicURL, e.bucket)
	if code, body := httpDo(t, "GET", presigned, nil, nil); code != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("presigned GET: HTTP %d, equal=%v", code, bytes.Equal(body, payload))
	}

	if !storage.UploadGrantsEnabled() {
		if _, err := storage.GenerateUploadURL(int64(len(payload)), sha256Hex(payload)); err == nil {
			t.Fatal("GenerateUploadURL with checksum=none succeeded")
		}
		return
	}
	grant, err := storage.GenerateUploadURL(int64(len(payload)), sha256Hex(payload))
	if err != nil {
		t.Fatalf("GenerateUploadURL: %v", err)
	}
	checkHost(t, grant.UploadURL, storage.publicURL, e.bucket)
	checkHost(t, grant.DownloadURL, storage.publicURL, e.bucket)
	if grant.ObjectID == "" || grant.Headers["x-amz-checksum-sha256"] == "" {
		t.Fatalf("grant: object id %q headers %v", grant.ObjectID, grant.Headers)
	}
	if err := storage.ValidateStorageURL(grant.DownloadURL); err != nil {
		t.Fatalf("ValidateStorageURL(own download URL): %v", err)
	}
	wrong := bytes.Repeat([]byte("MINIO"), 1<<18)
	if code, body := httpRefused(t, "PUT", grant.UploadURL, wrong, grant.Headers); code < 400 {
		t.Fatalf("wrong body of the right size accepted: HTTP %d %s", code, body)
	}
	if code, body := httpRefused(t, "PUT", grant.UploadURL, payload[:len(payload)-1], grant.Headers); code < 400 {
		t.Fatalf("short body accepted: HTTP %d %s", code, body)
	}
	if code, body := httpDo(t, "PUT", grant.UploadURL, payload, grant.Headers); code != http.StatusOK {
		t.Fatalf("grant PUT: HTTP %d %s", code, body)
	}
	got, err = storage.Get(grant.ObjectID, 0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Get(grant object): err=%v equal=%v", err, bytes.Equal(got, payload))
	}
	if code, body := httpDo(t, "GET", grant.DownloadURL, nil, nil); code != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("grant GET: HTTP %d", code)
	}
}

func sha256Hex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func checkHost(t *testing.T, raw, endpoint, bucket string) {
	t.Helper()
	pu, _ := url.Parse(raw)
	eu, _ := url.Parse(endpoint)
	if pu.Scheme != eu.Scheme || pu.Host != eu.Host || !strings.HasPrefix(pu.Path, "/"+bucket+"/") {
		t.Fatalf("presigned URL %s://%s%s is not path-style on %s", pu.Scheme, pu.Host, pu.Path, endpoint)
	}
}

// httpRefused is httpDo for a request storage must refuse: a server that
// rejects the body and closes the connection mid-upload counts as a refusal
// (status 499).
func httpRefused(t *testing.T, method, raw string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	code, data, err := httpSend(method, raw, body, headers)
	if err != nil {
		return 499, []byte(err.Error())
	}
	return code, data
}

func httpDo(t *testing.T, method, raw string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	code, data, err := httpSend(method, raw, body, headers)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	return code, data
}

func httpSend(method, raw string, body []byte, headers map[string]string) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, raw, rd)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data, nil
}

// TestS3CompatCustomEndpoint: a custom endpoint, path style by default, static
// keys, no region configured.
func TestS3CompatCustomEndpoint(t *testing.T) {
	e := s3compat(t)
	storage, err := NewS3Storage(e.bucket, S3Config{
		EndpointURL:     e.endpoint,
		AccessKeyID:     e.access,
		SecretAccessKey: e.secret,
		AllowPlainHTTP:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if storage.Region() != "us-east-1" {
		t.Errorf("region %q", storage.Region())
	}
	exerciseAllRoles(t, e, storage)
}

// TestS3CompatFromEnv: the VGI_STORAGE_S3_* settings, including the startup
// bucket check, an explicit prefix and a public endpoint that differs from
// the internal one (localhost vs 127.0.0.1, the same server).
func TestS3CompatFromEnv(t *testing.T) {
	e := s3compat(t)
	eu, _ := url.Parse(e.endpoint)
	public := eu.Scheme + "://localhost:" + eu.Port()
	env := map[string]string{
		EnvBucket: e.bucket, EnvEndpoint: e.endpoint, EnvPublicEndpoint: public, EnvPrefix: "vgi-env/",
		EnvAccessKeyID: e.access, EnvSecretAccessKey: e.secret, EnvVerify: "1", EnvAllowPlainHTTP: "1",
		EnvURLTTLSeconds: "1800",
	}
	storage, err := FromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if storage.prefix != "vgi-env/" {
		t.Errorf("prefix %q", storage.prefix)
	}
	exerciseAllRoles(t, e, storage)
	grant, err := storage.GenerateUploadURL(1, sha256Hex([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	if q, _ := url.Parse(grant.UploadURL); q.Query().Get("X-Amz-Expires") != "1800" {
		t.Errorf("X-Amz-Expires %q, want 1800 (static keys never expire)", q.Query().Get("X-Amz-Expires"))
	}
	if err := storage.ValidateStorageURL(strings.Replace(grant.DownloadURL, "localhost", "127.0.0.2", 1)); err == nil {
		t.Error("a URL on another host validated")
	}

	env[EnvBucket] = "vgi-no-such-bucket-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := FromEnv(func(k string) string { return env[k] }); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("missing bucket: %v", err)
	}
	env[EnvBucket] = e.bucket
	env[EnvSecretAccessKey] = "wrong-secret"
	if _, err := FromEnv(func(k string) string { return env[k] }); err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Errorf("bad secret: %v", err)
	}
}

// TestS3CompatSessionToken: temporary credentials from MinIO's STS AssumeRole,
// passed explicitly as key + secret + session token.
func TestS3CompatSessionToken(t *testing.T) {
	e := s3compat(t)
	creds := e.assumeRole(t, 900)
	storage, err := NewS3Storage(e.bucket, S3Config{
		EndpointURL:     e.endpoint,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		AllowPlainHTTP:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	exerciseAllRoles(t, e, storage)
	// The session token travels in the presigned URL.
	grant, err := storage.GenerateUploadURL(1, sha256Hex([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	if q, _ := url.Parse(grant.UploadURL); q.Query().Get("X-Amz-Security-Token") == "" {
		t.Error("presigned URL carries no X-Amz-Security-Token")
	}
}

// TestS3CompatCredentialProcessTTLCap: temporary credentials from the AWS
// default chain (a credential_process profile) that expire soon. URLs never
// outlive them (min(TTL, expiry - 5 min refresh window - 60 s)), and an
// upload grant, which needs 15 minutes, refreshes them first.
func TestS3CompatCredentialProcessTTLCap(t *testing.T) {
	e := s3compat(t)
	creds := e.assumeRole(t, 3600)
	dir := t.TempDir()
	// The process reports an expiry 8 minutes out (sooner than the real
	// one), and counts its calls.
	expires := time.Now().Add(8 * time.Minute).UTC().Format(time.RFC3339)
	out, _ := json.Marshal(map[string]any{
		"Version": 1, "AccessKeyId": creds.AccessKeyID, "SecretAccessKey": creds.SecretAccessKey,
		"SessionToken": creds.SessionToken, "Expiration": expires,
	})
	if err := os.WriteFile(filepath.Join(dir, "creds.json"), out, 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "creds.sh")
	if err := os.WriteFile(script, []byte(fmt.Sprintf("#!/bin/sh\necho x >> %q\ncat %q\n",
		filepath.Join(dir, "calls"), filepath.Join(dir, "creds.json"))), 0o700); err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(dir, "config")
	if err := os.WriteFile(conf, []byte("[profile vgi]\ncredential_process = "+script+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", conf)
	t.Setenv("AWS_PROFILE", "vgi")
	storage, err := NewS3Storage(e.bucket, S3Config{EndpointURL: e.endpoint, AllowPlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	presigned, err := storage.Upload([]byte("ttl"), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	q, _ := url.Parse(presigned)
	ttl, _ := strconv.Atoi(q.Query().Get("X-Amz-Expires"))
	// 8 min - 5 min refresh window - 60 s = 120 s.
	if ttl <= 0 || ttl > 120 {
		t.Errorf("result URL X-Amz-Expires=%d, want <= 120 under credentials expiring in 8 minutes", ttl)
	}
	if code, _ := httpDo(t, "GET", presigned, nil, nil); code != http.StatusOK {
		t.Errorf("capped URL GET: HTTP %d", code)
	}
	calls := func() int { b, _ := os.ReadFile(filepath.Join(dir, "calls")); return bytes.Count(b, []byte("x")) }
	// One retrieval, then one refresh: the URL needs 5 minutes and the
	// credentials allowed 2.
	if calls() != 2 {
		t.Errorf("credential_process ran %d times for the first URL, want 2 (retrieve + one refresh)", calls())
	}
	// An upload grant needs 15 minutes, but the credentials were just
	// refreshed and are as short-lived as before: no refresh storm.
	for i := 0; i < 2; i++ {
		if _, err := storage.GenerateUploadURL(3, sha256Hex([]byte("ttl"))); err != nil {
			t.Fatal(err)
		}
	}
	if calls() != 2 {
		t.Errorf("grants refreshed again (%d calls in all)", calls())
	}
}

// TestS3CompatChecksumNone: no upload grants, but results and objects still work
// with the SDK's default checksums off.
func TestS3CompatChecksumNone(t *testing.T) {
	e := s3compat(t)
	storage, err := NewS3Storage(e.bucket, S3Config{
		EndpointURL: e.endpoint, AccessKeyID: e.access, SecretAccessKey: e.secret, Checksum: ChecksumNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if storage.UploadGrantsEnabled() {
		t.Fatal("checksum=none vends upload grants")
	}
	exerciseAllRoles(t, e, storage)
}
