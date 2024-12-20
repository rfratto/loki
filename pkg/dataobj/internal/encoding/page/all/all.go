// Package all imports all page encoders, registering them with the page
// package.
package all

import (
	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/bitmap" // bitmap encoding
	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/delta"  // delta encoding
	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page/plain"  // plain encoding
)
