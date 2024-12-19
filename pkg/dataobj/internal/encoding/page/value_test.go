package page_test

import (
	"math"
	"testing"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"
)

func Benchmark_Value(b *testing.B) {
	b.Run("int64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sv := page.Int64Value(math.MaxInt64)
			if sv.Type() != datasetmd.VALUE_TYPE_INT64 {
				b.Fail()
			}
		}
	})

	b.Run("uint64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sv := page.Uint64Value(math.MaxUint64)
			if sv.Type() != datasetmd.VALUE_TYPE_UINT64 {
				b.Fail()
			}
		}
	})

	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sv := page.StringValue("hello, world! this is a very long string to check to see if long strings cause allocations")
			if sv.Type() != datasetmd.VALUE_TYPE_STRING {
				b.Fail()
			}
		}
	})
}
