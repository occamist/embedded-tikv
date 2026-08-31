//go:build unix

package embeddedtikv

import (
	"fmt"
	"io"
	"maps"
	"strings"

	"github.com/BurntSushi/toml"
)

// writeTOML renders a flat map of dotted configuration keys as a TOML document.
//
// Configuration here is addressed by dotted path ("storage.block-cache.capacity"), but the
// encoder builds tables from nested maps — handed dotted keys directly it emits them as literal
// quoted names, which neither TiKV nor PD reads. So the paths are expanded first and the encoder
// does the rest: string escaping, quoting keys that need it, and keeping floats floats.
func writeTOML(w io.Writer, config map[string]any) error {
	nested, err := nestConfig(config)
	if err != nil {
		return err
	}

	return toml.NewEncoder(w).Encode(nested)
}

// nestConfig expands dotted paths into the nested tables the encoder expects, reporting any
// path that collides with another rather than silently dropping one of them.
func nestConfig(config map[string]any) (map[string]any, error) {
	root := map[string]any{}

	// Sorted so that a collision is reported against the same key on every run.
	for _, key := range sortedKeys(config) {
		segments := strings.Split(key, ".")
		table := root

		for i, segment := range segments[:len(segments)-1] {
			if segment == "" {
				return nil, fmt.Errorf("embedded-tikv: invalid configuration key %q", key)
			}

			switch existing := table[segment].(type) {
			case nil:
				next := map[string]any{}
				table[segment] = next
				table = next
			case map[string]any:
				table = existing
			default:
				return nil, fmt.Errorf("embedded-tikv: configuration key %q conflicts with %q",
					key, strings.Join(segments[:i+1], "."))
			}
		}

		leaf := segments[len(segments)-1]
		if leaf == "" {
			return nil, fmt.Errorf("embedded-tikv: invalid configuration key %q", key)
		}

		if _, isTable := table[leaf].(map[string]any); isTable {
			return nil, fmt.Errorf("embedded-tikv: configuration key %q is also used as a table", key)
		}

		table[leaf] = config[key]
	}

	return root, nil
}

// mergeConfig layers overrides on top of defaults without mutating either.
// Both are flat dotted-key maps, so a plain key-wise overwrite is the whole merge.
func mergeConfig(defaults, overrides map[string]any) map[string]any {
	merged := make(map[string]any, len(defaults)+len(overrides))
	maps.Copy(merged, defaults)
	maps.Copy(merged, overrides)
	return merged
}
