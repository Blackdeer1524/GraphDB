package utils

import (
	"encoding/json"
	"fmt"

	"github.com/go-faster/jx"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// JXMapToAnyMap converts map[string]jx.Raw to map[string]*anypb.Any
func JXMapToAnyMap(m map[string]jx.Raw) (map[string]*anypb.Any, error) {
	out := make(map[string]*anypb.Any, len(m))

	for k, raw := range m {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		pbVal, err := structpb.NewValue(v)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		anyVal, err := anypb.New(pbVal)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		out[k] = anyVal
	}

	return out, nil
}
