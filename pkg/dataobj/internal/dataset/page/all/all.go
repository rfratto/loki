// Package all imports all page encoders, registering them with the page
// package.
package all

import (
	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page/bitmap" // bitmap encoding
	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page/delta"  // delta encoding
	_ "github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page/plain"  // plain encoding
)
