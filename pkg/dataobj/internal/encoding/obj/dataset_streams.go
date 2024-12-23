package obj

import (
	"context"
	"fmt"

	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/column"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset/page"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/filemd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/metadata/streamsmd"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
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

func (d *streamsDataset) ListColumns(ctx context.Context) result.Seq[dataset.Column] {
	return result.Iter(func(yield func(dataset.Column) bool) error {
		columnDescs, err := d.dec.Columns(ctx, d.section)
		if err != nil {
			return err
		}

		for _, desc := range columnDescs {
			info := &column.Info{
				Name: desc.Info.Name,
				Type: desc.Info.ValueType,

				RowsCount:        int(desc.Info.RowsCount),
				UncompressedSize: int(desc.Info.UncompressedSize),
				CompressedSize:   int(desc.Info.CompressedSize),

				Compression: desc.Info.Compression,

				Statistics: desc.Info.Statistics,
			}
			column := &streamsColumn{info: info, ds: d, desc: desc}
			if !yield(column) {
				return nil
			}
		}

		return nil
	})
}

func (d *streamsDataset) ListPages(ctx context.Context, columns []dataset.Column) result.Seq[dataset.Pages] {
	return result.Iter(func(yield func(dataset.Pages) bool) error {
		for _, column := range columns {
			c, ok := column.(*streamsColumn)
			if !ok {
				return fmt.Errorf("unrecognized column %v", column)
			}

			pages, err := result.Collect(c.Pages(ctx))
			if err != nil {
				return err
			} else if !yield(pages) {
				return nil
			}
		}

		return nil
	})
}

func (d *streamsDataset) ReadPages(ctx context.Context, pages []dataset.Page) result.Seq[page.Data] {
	return result.Iter(func(yield func(page.Data) bool) error {
		var descs = make([]*ColumnPageDesc, len(pages))
		for i, p := range pages {
			p, ok := p.(*streamsPage)
			if !ok {
				return fmt.Errorf("unrecognized page %v", p)
			}
			descs[i] = p.desc
		}

		for res := range d.dec.ReadPages(ctx, descs) {
			if res.Err() != nil || !yield(res.MustValue().Data) {
				return res.Err()
			}
		}

		return nil
	})
}

type streamsColumn struct {
	info *column.Info
	ds   *streamsDataset
	desc *streamsmd.ColumnDesc
}

func (c *streamsColumn) Info() *column.Info {
	return c.info
}

func (c *streamsColumn) Pages(ctx context.Context) result.Seq[dataset.Page] {
	return result.Iter(func(yield func(dataset.Page) bool) error {
		descs, err := c.ds.dec.Pages(ctx, c.desc)
		if err != nil {
			return err
		}

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

			if !yield(&streamsPage{info: info, ds: c.ds, desc: desc}) {
				return nil
			}
		}

		return nil
	})
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
	for res := range p.ds.dec.ReadPages(ctx, []*ColumnPageDesc{p.desc}) {
		data, err := res.Value()
		if err != nil {
			return nil, err
		} else {
			return data.Data, nil
		}
	}
	return nil, fmt.Errorf("no page data")
}
