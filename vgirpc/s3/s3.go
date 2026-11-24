// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package vgis3 provides an S3-backed storage for vgi-rpc: result
// externalization (ExternalStorage), upload grants (UploadURLProvider) and
// object storage (ObjectStore) on AWS S3 or any S3-compatible service (MinIO,
// Ceph, Cloudflare R2's S3 API, Wasabi, Backblaze B2, LocalStack).
//
// Usage:
//
//	storage, err := vgis3.NewS3Storage("my-bucket", vgis3.S3Config{
//	    Prefix: "vgi-rpc/",
//	})
//	server.SetExternalLocation(vgirpc.DefaultExternalLocationConfig(storage))
//
// or, configured by the VGI_STORAGE_S3_* environment (direct-storage
// contract v1), [FromEnv].
package vgis3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Checksum modes ([S3Config.Checksum], VGI_STORAGE_S3_CHECKSUM).
const (
	// ChecksumSHA256 sends x-amz-checksum-sha256 on every upload and signs it
	// into upload grants, so storage refuses a body that does not match.
	ChecksumSHA256 = "sha256"
	// ChecksumNone is for S3-compatible services that do not enforce
	// x-amz-checksum-sha256: no upload grants are vended (argument references
	// and large requests stay inline), but results are still externalized
	// (the client verifies their sha256) and objects stored (the worker
	// verifies them on fetch). The SDK's own default checksums (CRC32) are
	// turned off too.
	ChecksumNone = "none"
)

const (
	// maxPresignExpiry is SigV4's limit on X-Amz-Expires (7 days).
	maxPresignExpiry = 7 * 24 * time.Hour
	// credentialSafetyMargin: a URL must stop being offered this long before
	// the credentials that signed it expire.
	credentialSafetyMargin = 60 * time.Second
	// credentialRefreshWindow: temporary credentials are refreshed this long
	// before they expire.
	credentialRefreshWindow = 5 * time.Minute
	// grantMinLifetime is the lifetime an upload grant needs; credentials
	// expiring sooner are refreshed before signing.
	grantMinLifetime = 15 * time.Minute
	// resultMinLifetime is the lifetime a result's download URL needs.
	resultMinLifetime = 5 * time.Minute
)

// S3Config configures the S3 storage backend.
type S3Config struct {
	// Prefix is the S3 key prefix. Objects go under <Prefix>rpc/ (results,
	// upload grants, StateRef and argument objects) and <Prefix>fs/ (function
	// storage, [S3Storage.FunctionStore]); add lifecycle rules expiring rpc/
	// after 1 day and fs/ after 2 days. Default: "vgi-rpc/" unless
	// ExactPrefix is set.
	Prefix string
	// ExactPrefix uses Prefix exactly as given, even when it is empty
	// (VGI_STORAGE_S3_PREFIX unset means no prefix).
	ExactPrefix bool
	// PresignExpirySeconds is the lifetime of pre-signed URLs. Default: 3600
	// (1 hour); at most 7 days. Under temporary credentials a URL never
	// outlives them: its lifetime is min(this, expiry - now - 60s), where
	// expiry is the credential cache's refresh time (5 minutes before the
	// credentials really expire), so URLs stop being offered 6 minutes
	// before their signature dies. Credentials expiring within an upload
	// grant's 15 minutes are refreshed before signing.
	PresignExpirySeconds int
	// Region is the signing region. If empty: AWS_REGION, then the shared
	// config profile, then "us-east-1" (MinIO, Ceph and LocalStack accept it;
	// R2's own name is "auto").
	Region string
	// EndpointURL overrides the S3 endpoint for S3-compatible services
	// (MinIO, Ceph, LocalStack, Cloudflare R2, Wasabi, Backblaze B2);
	// http:// endpoints work. Empty means AWS.
	EndpointURL string
	// PublicEndpointURL, when set, is used only for the pre-signed URLs handed
	// to clients (upload grants, result downloads), for a worker that reaches
	// storage through a different address than clients do (e.g. Docker's
	// http://minio:9000 vs https://s3.example.com). Default: EndpointURL.
	PublicEndpointURL string
	// VirtualHostedStyle addresses buckets as <bucket>.<host> on an
	// EndpointURL. The default with an EndpointURL is path-style
	// (<host>/<bucket>), which MinIO and Ceph need; without an EndpointURL
	// the SDK picks (virtual-hosted on AWS).
	VirtualHostedStyle bool
	// ForcePathStyle uses path-style addressing even without an EndpointURL.
	ForcePathStyle bool
	// AccessKeyID, SecretAccessKey and SessionToken are static credentials
	// (e.g. an R2 API token, or keys from STS). When AccessKeyID is empty the
	// SDK's default chain applies: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/
	// AWS_SESSION_TOKEN, the shared config/credentials profile (AWS_PROFILE,
	// role_arn/source_profile, credential_process, SSO), web identity
	// (AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN), ECS/EKS container
	// credentials and EC2 IMDSv2. Temporary credentials are refreshed 5
	// minutes before they expire.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// SessionExpires is when the static SessionToken expires
	// (VGI_STORAGE_S3_SESSION_EXPIRES). Set, pre-signed URLs are capped at it
	// like any temporary credentials. Unset with a SessionToken, URL
	// lifetimes cannot be capped ([S3Storage.StartupWarnings] says so).
	SessionExpires time.Time
	// Checksum is ChecksumSHA256 (default) or ChecksumNone.
	Checksum string
	// AllowPlainHTTP lets [S3Storage.ValidateStorageURL] accept http:// URLs
	// on the configured endpoints (VGI_STORAGE_ALLOW_PLAIN_HTTP).
	AllowPlainHTTP bool
	// StallTimeout fails a transfer that sends or receives no byte for this
	// long; a slow transfer that keeps moving is never cut off (there is no
	// whole-request deadline). Default: VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS,
	// else 30s. The connect timeout is 30s. Failed requests (connection
	// errors, stalls, 5xx, throttling) are retried by the SDK's standard
	// retryer: 3 attempts, jittered exponential backoff capped at 5s.
	StallTimeout time.Duration
}

// S3Storage implements vgirpc.ExternalStorage, vgirpc.UploadURLProvider and
// vgirpc.ObjectStore using S3.
type S3Storage struct {
	bucket        string
	root          string // the configured prefix
	prefix        string // root + "rpc/"
	warnings      []string
	region        string
	endpoint      string // where the worker reaches storage ("" = AWS)
	publicURL     string // endpoint in pre-signed URLs ("" = AWS)
	checksum      string
	presignExpiry time.Duration
	allowHTTP     bool
	client        *s3.Client
	presignClient *s3.PresignClient // on the public endpoint
	creds         aws.CredentialsProvider

	verifyMu sync.Mutex
	verified bool

	refreshMu        sync.Mutex
	refreshedKey     string    // access key id of the last forced refresh
	refreshedExpires time.Time // and its (cache-adjusted) expiry
}

// noteRefresh remembers the credentials a forced refresh returned.
func (s *S3Storage) noteRefresh(c aws.Credentials) {
	s.refreshMu.Lock()
	s.refreshedKey, s.refreshedExpires = c.AccessKeyID, c.Expires
	s.refreshMu.Unlock()
}

// isFreshRefresh reports whether c are the credentials the last forced
// refresh returned (refreshing again would only fetch the same lifetime).
func (s *S3Storage) isFreshRefresh(c aws.Credentials) bool {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	return c.AccessKeyID == s.refreshedKey && c.Expires.Equal(s.refreshedExpires)
}

// NewS3Storage creates a new S3-backed storage.
func NewS3Storage(bucket string, cfg S3Config) (*S3Storage, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, errors.New("S3 bucket is empty")
	}
	prefix := cfg.Prefix
	if prefix == "" && !cfg.ExactPrefix {
		prefix = "vgi-rpc/"
	}
	expiry := time.Duration(cfg.PresignExpirySeconds) * time.Second
	if expiry <= 0 {
		expiry = time.Hour
	}
	expiry = min(expiry, maxPresignExpiry)
	checksum := strings.ToLower(strings.TrimSpace(cfg.Checksum))
	switch checksum {
	case "":
		checksum = ChecksumSHA256
	case ChecksumSHA256, ChecksumNone:
	default:
		return nil, fmt.Errorf("S3 checksum %q: expected sha256 or none", cfg.Checksum)
	}
	for _, ep := range []string{cfg.EndpointURL, cfg.PublicEndpointURL} {
		if ep == "" {
			continue
		}
		if u, err := url.Parse(ep); err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("S3 endpoint %q: want http(s)://host[:port]", ep)
		}
	}
	if (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == "") {
		return nil, errors.New("S3 static credentials need both an access key id and a secret access key")
	}

	opts := []func(*config.LoadOptions) error{
		config.WithCredentialsCacheOptions(func(o *aws.CredentialsCacheOptions) {
			o.ExpiryWindow = credentialRefreshWindow
		}),
	}
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	var warnings []string
	if cfg.AccessKeyID != "" {
		var provider aws.CredentialsProvider = credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
		if !cfg.SessionExpires.IsZero() {
			provider = expiringStaticProvider{aws.Credentials{
				AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey, SessionToken: cfg.SessionToken,
				Source: "StaticCredentials", CanExpire: true, Expires: cfg.SessionExpires,
			}}
		} else if cfg.SessionToken != "" {
			warnings = append(warnings, "an explicit S3 session token (VGI_STORAGE_S3_SESSION_TOKEN) has no expiry "+
				"(VGI_STORAGE_S3_SESSION_EXPIRES), so pre-signed URL lifetimes cannot be capped at it: URLs may outlive "+
				"the session and fail; set VGI_STORAGE_S3_SESSION_EXPIRES, or use credential_process or the standard AWS chain")
		}
		opts = append(opts, config.WithCredentialsProvider(provider))
	}
	stall := stallTimeout(cfg.StallTimeout)
	opts = append(opts,
		config.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			tr.DialContext = stallDialer(stall)
			tr.TLSHandshakeTimeout = storageConnectTimeout
		})),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 3
				o.MaxBackoff = 5 * time.Second
			})
		}),
	)
	if checksum == ChecksumNone {
		// No SDK-default CRC32 either: services without flexible checksums
		// reject the aws-chunked trailer it uses.
		opts = append(opts,
			config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
			config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired))
	}
	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	if awsCfg.Region == "" {
		awsCfg.Region = "us-east-1"
	}

	pathStyle := cfg.ForcePathStyle || (cfg.EndpointURL != "" && !cfg.VirtualHostedStyle)
	clientFor := func(endpoint string) *s3.Client {
		return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if endpoint != "" {
				o.BaseEndpoint = aws.String(endpoint)
			}
			o.UsePathStyle = pathStyle
		})
	}
	client := clientFor(cfg.EndpointURL)
	public := cfg.PublicEndpointURL
	if public == "" {
		public = cfg.EndpointURL
	}
	presignBase := client
	if public != cfg.EndpointURL {
		presignBase = clientFor(public)
	}

	return &S3Storage{
		bucket:        bucket,
		root:          prefix,
		prefix:        prefix + "rpc/",
		warnings:      warnings,
		region:        awsCfg.Region,
		endpoint:      cfg.EndpointURL,
		publicURL:     public,
		checksum:      checksum,
		presignExpiry: expiry,
		allowHTTP:     cfg.AllowPlainHTTP,
		client:        client,
		presignClient: s3.NewPresignClient(presignBase),
		creds:         awsCfg.Credentials,
	}, nil
}

// Bucket is the configured bucket.
func (s *S3Storage) Bucket() string { return s.bucket }

// StartupWarnings are configuration warnings a worker should log at startup
// (never containing secrets): an explicit session token without an expiry.
func (s *S3Storage) StartupWarnings() []string { return append([]string(nil), s.warnings...) }

// expiringStaticProvider is static credentials with a known expiry
// (VGI_STORAGE_S3_SESSION_EXPIRES), so the URL lifetime cap applies.
type expiringStaticProvider struct{ creds aws.Credentials }

func (p expiringStaticProvider) Retrieve(context.Context) (aws.Credentials, error) {
	return p.creds, nil
}

// Region is the signing region in use.
func (s *S3Storage) Region() string { return s.region }

// Checksum is the checksum mode (ChecksumSHA256 or ChecksumNone).
func (s *S3Storage) Checksum() string { return s.checksum }

// UploadGrantsEnabled reports whether this storage vends upload grants: not
// with ChecksumNone, since storage could not bind the body to its sha256.
// vgirpc then advertises no upload-URL support.
func (s *S3Storage) UploadGrantsEnabled() bool { return s.checksum == ChecksumSHA256 }

// presignOptions returns the credentials and lifetime for a pre-signed URL
// that must stay usable for at least need. Under temporary credentials the
// lifetime is capped at their expiry minus 60 s, and credentials expiring
// within need (+60 s) are refreshed first; the URL is signed with exactly the
// credentials its lifetime was computed from.
func (s *S3Storage) presignOptions(ctx context.Context, need time.Duration) ([]func(*s3.PresignOptions), time.Time, error) {
	creds, err := s.creds.Retrieve(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("S3 credentials: %w", err)
	}
	ttl := s.presignExpiry
	if creds.CanExpire {
		remaining := time.Until(creds.Expires) - credentialSafetyMargin
		if cache, ok := s.creds.(*aws.CredentialsCache); ok && remaining < need && !s.isFreshRefresh(creds) {
			// Refresh once. If the fresh credentials are still too short-lived
			// (a 15-minute STS session), sign with what they allow and do not
			// refresh again until they change.
			cache.Invalidate()
			if creds, err = s.creds.Retrieve(ctx); err != nil {
				return nil, time.Time{}, fmt.Errorf("S3 credentials refresh: %w", err)
			}
			s.noteRefresh(creds)
			remaining = time.Until(creds.Expires) - credentialSafetyMargin
		}
		if !creds.CanExpire {
			remaining = ttl
		}
		if remaining <= 0 {
			return nil, time.Time{}, fmt.Errorf("S3 credentials (%s) expire at %s, too soon to sign a URL",
				creds.Source, creds.Expires.UTC().Format(time.RFC3339))
		}
		ttl = min(ttl, remaining)
	}
	// SigV4 counts X-Amz-Expires in whole seconds.
	ttl = max(ttl.Truncate(time.Second), time.Second)
	signed := creds
	return []func(*s3.PresignOptions){
		s3.WithPresignExpires(ttl),
		s3.WithPresignClientFromClientOptions(func(o *s3.Options) {
			o.Credentials = credentials.StaticCredentialsProvider{Value: signed}
		}),
	}, time.Now().Add(ttl).UTC(), nil
}

// Upload implements vgirpc.ExternalStorage: PUT data (with its sha256 unless
// ChecksumNone) and return a pre-signed GET URL on the public endpoint.
func (s *S3Storage) Upload(data []byte, schema *arrow.Schema, contentEncoding string) (string, error) {
	ctx := context.Background()
	if err := s.ensureVerified(ctx); err != nil {
		return "", err
	}
	key := s.prefix + generateUUID()

	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/vnd.apache.arrow.stream"),
	}
	if s.checksum == ChecksumSHA256 {
		sum := sha256.Sum256(data)
		input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
	}
	if contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	if _, err := s.client.PutObject(ctx, input); err != nil {
		return "", fmt.Errorf("S3 PutObject: %w", err)
	}

	popts, _, err := s.presignOptions(ctx, resultMinLifetime)
	if err != nil {
		return "", err
	}
	presigned, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, popts...)
	if err != nil {
		return "", fmt.Errorf("S3 presign: %w", err)
	}
	return presigned.URL, nil
}

// GenerateUploadURL implements vgirpc.UploadURLProvider: a pre-signed PUT for
// exactly size bytes whose SHA-256 is sha256Hex (SigV4 signs Content-Length
// and x-amz-checksum-sha256, so storage refuses any other body), a pre-signed
// GET for the same object, both on the public endpoint, and the object's key
// as its ObjectID. It needs ChecksumSHA256.
func (s *S3Storage) GenerateUploadURL(size int64, sha256Hex string) (vgirpc.UploadURL, error) {
	if !s.UploadGrantsEnabled() {
		return vgirpc.UploadURL{}, errors.New("S3 upload grants are disabled (VGI_STORAGE_S3_CHECKSUM=none)")
	}
	if size < 0 {
		return vgirpc.UploadURL{}, fmt.Errorf("upload size %d is negative", size)
	}
	raw, err := hex.DecodeString(sha256Hex)
	if err != nil || len(raw) != sha256.Size || strings.ToLower(sha256Hex) != sha256Hex {
		return vgirpc.UploadURL{}, errors.New("sha256 must be 64 lowercase hex characters")
	}
	ctx := context.Background()
	if err := s.ensureVerified(ctx); err != nil {
		return vgirpc.UploadURL{}, err
	}
	checksum := base64.StdEncoding.EncodeToString(raw)
	key, err := s.newObjectKey(".arrow")
	if err != nil {
		return vgirpc.UploadURL{}, err
	}
	popts, expires, err := s.presignOptions(ctx, grantMinLifetime)
	if err != nil {
		return vgirpc.UploadURL{}, err
	}
	// No header hoisting: by default the presigner moves x-amz-checksum-sha256
	// into the query string unsigned, and S3-compatible stores (MinIO) then
	// accept any body. As a signed header storage enforces it.
	put, err := s.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(s.bucket),
		Key:            aws.String(key),
		ContentLength:  aws.Int64(size),
		ChecksumSHA256: aws.String(checksum),
	}, append(popts, func(o *s3.PresignOptions) {
		o.Presigner = v4.NewSigner(func(so *v4.SignerOptions) { so.DisableHeaderHoisting = true })
	})...)
	if err != nil {
		return vgirpc.UploadURL{}, fmt.Errorf("S3 presign PUT: %w", err)
	}
	get, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, popts...)
	if err != nil {
		return vgirpc.UploadURL{}, fmt.Errorf("S3 presign GET: %w", err)
	}
	headers := map[string]string{}
	for name, values := range put.SignedHeader {
		switch strings.ToLower(name) {
		case "host", "content-length":
			continue
		}
		if len(values) > 0 {
			headers[strings.ToLower(name)] = values[0]
		}
	}
	if headers["x-amz-checksum-sha256"] != checksum {
		return vgirpc.UploadURL{}, errors.New("S3 presign PUT did not sign x-amz-checksum-sha256")
	}
	return vgirpc.UploadURL{
		UploadURL:   put.URL,
		DownloadURL: get.URL,
		ExpiresAt:   expires,
		Headers:     headers,
		ObjectID:    key,
	}, nil
}

// ValidateStorageURL accepts only URLs on this storage's endpoints (the
// internal and the public one): https://, or http:// with AllowPlainHTTP.
// Without an endpoint (AWS), any https host under amazonaws.com. vgirpc uses
// it to pin inbound pointer fetches.
func (s *S3Storage) ValidateStorageURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return errors.New("invalid storage URL")
	}
	if u.User != nil {
		return errors.New("storage URL userinfo is not allowed")
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !s.allowHTTP {
			return errors.New("plain http storage URL refused (set VGI_STORAGE_ALLOW_PLAIN_HTTP=1 to allow it for the configured endpoint)")
		}
	default:
		return fmt.Errorf("storage URL scheme %q not allowed", u.Scheme)
	}
	host := strings.ToLower(u.Host)
	matched := false
	for _, ep := range []string{s.endpoint, s.publicURL} {
		if ep == "" {
			// AWS: regional, dual-stack and FIPS hosts are all under amazonaws.com.
			if u.Scheme == "https" && (strings.HasSuffix(strings.ToLower(u.Hostname()), ".amazonaws.com")) {
				matched = true
			}
			continue
		}
		e, _ := url.Parse(ep)
		if e.Scheme == u.Scheme && hostPort(e) == hostPort(u) {
			matched = true
		}
		// Virtual-hosted style puts the bucket in front of the endpoint host.
		if e.Scheme == u.Scheme && host == strings.ToLower(s.bucket+"."+e.Host) {
			matched = true
		}
	}
	if !matched {
		return fmt.Errorf("URL host %q is not the configured S3 endpoint", u.Host)
	}
	return nil
}

func hostPort(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	return strings.ToLower(u.Hostname()) + ":" + port
}

// Verify checks the bucket with a HEAD request and explains a failure: the
// bucket missing, access denied, or the bucket in another region (named, from
// the service's answer). It needs s3:ListBucket on the bucket.
func (s *S3Storage) Verify(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	if err == nil {
		s.verifyMu.Lock()
		s.verified = true
		s.verifyMu.Unlock()
		return nil
	}
	where := s.endpoint
	if where == "" {
		where = "AWS S3"
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.Response != nil {
		status := re.HTTPStatusCode()
		actual := re.Response.Header.Get("x-amz-bucket-region")
		switch {
		case actual != "" && actual != s.region:
			return fmt.Errorf("S3 bucket %q is in region %q but the worker signs for %q: set VGI_STORAGE_S3_REGION=%s",
				s.bucket, actual, s.region, actual)
		case status == http.StatusMovedPermanently:
			return fmt.Errorf("S3 bucket %q is not in region %q (the service redirected): set VGI_STORAGE_S3_REGION to the bucket's region",
				s.bucket, s.region)
		case status == http.StatusNotFound:
			return fmt.Errorf("S3 bucket %q does not exist at %s: check VGI_STORAGE_S3_BUCKET and VGI_STORAGE_S3_ENDPOINT",
				s.bucket, where)
		case status == http.StatusForbidden:
			source := ""
			if c, cerr := s.creds.Retrieve(ctx); cerr == nil && c.Source != "" {
				source = " (credentials from " + c.Source + ")"
			}
			return fmt.Errorf("S3 access denied to bucket %q at %s%s: the credentials need s3:ListBucket, s3:GetObject and s3:PutObject on it",
				s.bucket, where, source)
		}
		return fmt.Errorf("S3 bucket %q check at %s failed: HTTP %d", s.bucket, where, status)
	}
	return fmt.Errorf("S3 bucket %q check at %s failed: %w", s.bucket, where, err)
}

// ensureVerified runs [S3Storage.Verify] on first use; a failure is returned
// and retried on the next use, a success is remembered.
func (s *S3Storage) ensureVerified(ctx context.Context) error {
	s.verifyMu.Lock()
	done := s.verified
	s.verifyMu.Unlock()
	if done {
		return nil
	}
	return s.Verify(ctx)
}

// defaultMaxFetchBytes is the Get cap when the caller passes none (256 MiB,
// vgirpc's default inbound fetch cap).
const defaultMaxFetchBytes int64 = 256 * 1024 * 1024

// ErrObjectMissing is returned by Get when S3 reports the key absent
// (NoSuchKey / 404). It carries an ObjectMissing() method, which
// vgirpc.IsObjectMissing recognizes, so StateRef and argument-reference
// resolution map it to state_ref_missing / arguments_ref_missing.
var ErrObjectMissing error = objectMissingError{}

type objectMissingError struct{}

func (objectMissingError) Error() string       { return "S3 object is gone" }
func (objectMissingError) ObjectMissing() bool { return true }

func (s *S3Storage) newObjectKey(ext string) (string, error) { return randomKey(s.prefix, ext) }

// randomKey is prefix + 32 random hex characters + ext.
func randomKey(prefix, ext string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("S3 object key: %w", err)
	}
	return prefix + hex.EncodeToString(raw[:]) + ext, nil
}

// PutObject stores data under a fresh key and returns the key: the
// vgirpc.ObjectStore contract of protocol 3.4 (StateRef objects, argument
// references). A SigV4 PutObject with ContentLength and, unless ChecksumNone,
// ChecksumSHA256, so S3/R2 verify the body. Objects carry no TTL: operators
// MUST set a lifecycle rule (1-2 days) on the prefix; the worker never
// deletes.
func (s *S3Storage) PutObject(data []byte) (string, error) {
	ctx := context.Background()
	if err := s.ensureVerified(ctx); err != nil {
		return "", err
	}
	key, err := s.newObjectKey(".bin")
	if err != nil {
		return "", err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/octet-stream"),
	}
	if s.checksum == ChecksumSHA256 {
		sum := sha256.Sum256(data)
		input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
	}
	if _, err := s.client.PutObject(ctx, input); err != nil {
		return "", fmt.Errorf("S3 PutObject: %w", err)
	}
	return key, nil
}

// Get fetches the object a key names (vgirpc.ObjectStore), refusing more than
// maxBytes (<= 0: 256 MiB). A missing key is [ErrObjectMissing]. The caller
// verifies size and digest. Objects are addressed by key on the worker's own
// endpoint, so they survive credential rotation.
func (s *S3Storage) Get(objectID string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxFetchBytes
	}
	ctx := context.Background()
	if err := s.ensureVerified(ctx); err != nil {
		return nil, err
	}
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectID),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchKey", "NotFound", "404":
				return nil, ErrObjectMissing
			}
		}
		return nil, fmt.Errorf("S3 GetObject: %w", err)
	}
	defer resp.Body.Close()
	if resp.ContentLength != nil && *resp.ContentLength > maxBytes {
		return nil, fmt.Errorf("S3 object is %d bytes, over max_fetch_bytes (%d)", *resp.ContentLength, maxBytes)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("S3 GetObject read: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("S3 object exceeds max_fetch_bytes (%d)", maxBytes)
	}
	return data, nil
}

// Compile-time checks: S3Storage is every storage role protocol 3.4 asks of
// a backend.
var (
	_ vgirpc.ExternalStorage   = (*S3Storage)(nil)
	_ vgirpc.UploadURLProvider = (*S3Storage)(nil)
	_ vgirpc.ObjectStore       = (*S3Storage)(nil)
)

// generateUUID returns a random (version 4) UUID string.
func generateUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
