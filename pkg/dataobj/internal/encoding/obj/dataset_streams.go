package obj

import (
	"context"
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/encoding/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
)

// StreamsDataset returns a [dataset.Dataset] which reads columns and pages
// from the specified streams section of a data object.
func StreamsDataset(dec StreamsDecoder, section *filemd.SectionInfo) (dataset.Dataset, error) {
	if section.Type != filemd.SECTION_TYPE_STREAMS {
		return nil, fmt.Errorf("invalid section type %s", section.Type)
	}

	return &streamsDataset{dec: dec, section: section}, nil
}

type streamsDataset struct {
	dec     StreamsDecoder
	section *filemd.SectionInfo
}

func (d *streamsDataset) ListColumns(ctx context.Context) ([]dataset.Column, error) {
	columnDescs, err := d.dec.Columns(ctx, d.section)
	if err != nil {
		return nil, err
	}

	columns := make([]dataset.Column, 0, len(columnDescs))
	for _, desc := range columnDescs {
		columns = append(columns, streamsColumn{ds: d, desc: desc})
	}
	return columns, nil
}

func (d *streamsDataset) ListPages(ctx context.Context, columns []dataset.Column) ([]dataset.Pages, error) {
	var pagesList = make([]dataset.Pages, len(columns))

	for i, column := range columns {
		c, ok := column.(streamsColumn)
		if !ok {
			return nil, fmt.Errorf("unrecognized column %v", c)
		}
		pages, err := c.Pages(ctx)
		if err != nil {
			return nil, err
		}
		pagesList[i] = pages
	}

	return pagesList, nil
}

func (d *streamsDataset) ReadPages(ctx context.Context, pages []dataset.Page) ([]page.Data, error) {
	var descs = make([]*ColumnPageDesc, len(pages))
	for i, page := range pages {
		p, ok := page.(streamsPage)
		if !ok {
			return nil, fmt.Errorf("unrecognized page %v", p)
		}
		descs[i] = p.desc
	}

	var dataList = make([]page.Data, 0, len(pages))
	for page, err := range d.dec.ReadPages(ctx, descs) {
		if err != nil {
			return nil, err
		}
		dataList = append(dataList, page.Data)
	}
	return dataList, nil
}

type streamsColumn struct {
	ds   *streamsDataset
	desc *streamsmd.ColumnDesc
}

func (c streamsColumn) Info() *dataset.ColumnInfo {
	return &dataset.ColumnInfo{
		Name: c.desc.Info.Name,
		Type: c.desc.Info.ValueType,

		RowsCount:        int(c.desc.Info.RowsCount),
		UncompressedSize: int(c.desc.Info.UncompressedSize),
		CompressedSize:   int(c.desc.Info.CompressedSize),

		Compression: c.desc.Info.Compression,

		Statistics: c.desc.Info.Statistics,
	}
}

func (c streamsColumn) Pages(ctx context.Context) (dataset.Pages, error) {
	descs, err := c.ds.dec.Pages(ctx, c.desc)
	if err != nil {
		return nil, err
	}
	pages := make([]dataset.Page, 0, len(descs))
	for _, desc := range descs {
		pages = append(pages, streamsPage{ds: c.ds, desc: desc})
	}
	return pages, nil
}

type streamsPage struct {
	ds   *streamsDataset
	desc *ColumnPageDesc
}

func (p streamsPage) Info() *page.Info {
	return &page.Info{
		UncompressedSize: int(p.desc.Page.Info.UncompressedSize),
		CompressedSize:   int(p.desc.Page.Info.CompressedSize),
		CRC32:            p.desc.Page.Info.Crc32,
		RowCount:         int(p.desc.Page.Info.RowsCount),

		Value:       p.desc.Column.Info.ValueType,
		Compression: p.desc.Page.Info.Compression,
		Encoding:    p.desc.Page.Info.Encoding,
		Stats:       p.desc.Page.Info.Statistics,
	}
}

func (p streamsPage) Data(ctx context.Context) (page.Data, error) {
	for data, err := range p.ds.dec.ReadPages(ctx, []*ColumnPageDesc{p.desc}) {
		return data.Data, err
	}
	return nil, fmt.Errorf("no page data")
}
