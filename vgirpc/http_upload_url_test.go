// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// TestPublicUploadURLContractIsExported guards the __upload_url__ wire
// contract so intermediaries needn't copy it. Mirrors the Python
// reference's test_public_upload_url_contract_is_exported.
func TestPublicUploadURLContractIsExported(t *testing.T) {
	if UploadURLMethod != "__upload_url__" {
		t.Fatalf("expected __upload_url__, got %q", UploadURLMethod)
	}
	if MaxUploadURLCount != 100 {
		t.Fatalf("expected 100, got %d", MaxUploadURLCount)
	}

	if got := UploadURLParamsSchema.NumFields(); got != 1 {
		t.Fatalf("expected 1 params field, got %d", got)
	}
	if f := UploadURLParamsSchema.Field(0); f.Name != "count" || f.Type.ID() != arrow.INT64 {
		t.Fatalf("expected int64 count field, got %s", f)
	}

	wantNames := []string{"upload_url", "download_url", "expires_at"}
	if got := UploadURLResponseSchema.NumFields(); got != len(wantNames) {
		t.Fatalf("expected %d response fields, got %d", len(wantNames), got)
	}
	for i, name := range wantNames {
		if got := UploadURLResponseSchema.Field(i).Name; got != name {
			t.Fatalf("expected response field %d to be %q, got %q", i, name, got)
		}
	}
	if f := UploadURLResponseSchema.Field(2); f.Type.ID() != arrow.TIMESTAMP {
		t.Fatalf("expected timestamp expires_at, got %s", f.Type)
	}
}
