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
		info := &dataset.ColumnInfo{
			Name: desc.Info.Name,
			Type: desc.Info.ValueType,

			RowsCount:        int(desc.Info.RowsCount),
			UncompressedSize: int(desc.Info.UncompressedSize),
			CompressedSize:   int(desc.Info.CompressedSize),

			Compression: desc.Info.Compression,

			Statistics: desc.Info.Statistics,
		}
		columns = append(columns, &streamsColumn{info: info, ds: d, desc: desc})
	}
	return columns, nil
}

func (d *streamsDataset) ListPages(ctx context.Context, columns []dataset.Column) ([]dataset.Pages, error) {
	var pagesList = make([]dataset.Pages, len(columns))

	for i, column := range columns {
		c, ok := column.(*streamsColumn)
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
		p, ok := page.(*streamsPage)
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
	info *dataset.ColumnInfo
	ds   *streamsDataset
	desc *streamsmd.ColumnDesc
}

func (c *streamsColumn) Info() *dataset.ColumnInfo {
	return c.info
}

func (c *streamsColumn) Pages(ctx context.Context) (dataset.Pages, error) {
	descs, err := c.ds.dec.Pages(ctx, c.desc)
	if err != nil {
		return nil, err
	}
	pages := make([]dataset.Page, 0, len(descs))
	for _, desc := range descs {
		info := &page.Info{
			UncompressedSize: int(desc.Page.Info.UncompressedSize),
			CompressedSize:   int(desc.Page.Info.CompressedSize),
			CRC32:            desc.Page.Info.Crc32,
			RowCount:         int(desc.Page.Info.RowsCount),

			Value:       desc.Column.Info.ValueType,
			Compression: desc.Page.Info.Compression,
			Encoding:    desc.Page.Info.Encoding,
			Stats:       desc.Page.Info.Statistics,
		}
		pages = append(pages, &streamsPage{info: info, ds: c.ds, desc: desc})
	}
	return pages, nil
}

type streamsPage struct {
	info *page.Info
	ds   *streamsDataset
	desc *ColumnPageDesc
}

func (p *streamsPage) Info() *page.Info {
	return p.info
}

func (p *streamsPage) Data(ctx context.Context) (page.Data, error) {
	for data, err := range p.ds.dec.ReadPages(ctx, []*ColumnPageDesc{p.desc}) {
		return data.Data, err
	}
	return nil, fmt.Errorf("no page data")
}
