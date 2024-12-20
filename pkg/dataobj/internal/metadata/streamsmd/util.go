package streamsmd

import "github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/datasetmd"

// ColumnInfo returns a slice of [datasetmd.ColumnInfo] from the provided desc
// slice.
func ColumnInfos(desc []*ColumnDesc) []*datasetmd.ColumnInfo {
	res := make([]*datasetmd.ColumnInfo, len(desc))
	for i, d := range desc {
		res[i] = d.Info
	}
	return res
}
