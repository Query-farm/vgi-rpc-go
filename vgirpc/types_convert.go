// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
)

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toUint64(v any) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case uint:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case int64:
		return uint64(val), nil
	case int:
		return uint64(val), nil
	case int32:
		return uint64(val), nil
	case int16:
		return uint64(val), nil
	case int8:
		return uint64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}
