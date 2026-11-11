package azureblob

import (
	"context"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	core "github.com/nostalume/mountweave"
)

func (d *Driver) Enumerate(ctx context.Context, q core.NativeQuery) (core.Iterator, error) {
	if err := d.admit(ctx, q.Address(), true); err != nil {
		return nil, err
	}
	if !utf8.ValidString(q.Delimiter()) || !utf8.ValidString(q.StartAfter()) {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	return &iterator{d: d, prefix: d.prefix + q.Address().Value(), delimiter: q.Delimiter(), after: q.StartAfter()}, nil
}

type iterator struct {
	d                        *Driver
	prefix, delimiter, after string
	marker                   string
	flat                     *runtime.Pager[container.ListBlobsFlatResponse]
	hierarchy                *runtime.Pager[container.ListBlobsHierarchyResponse]
	done, closed             bool
}

func (i *iterator) Close() error {
	i.closed, i.flat, i.hierarchy = true, nil, nil
	return nil
}

func (i *iterator) Next(ctx context.Context, n int) ([]core.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.closed {
		return nil, io.ErrClosedPipe
	}
	if i.d.closed.Load() {
		return nil, core.Fail(core.MountUnavailable, nil)
	}
	if i.done {
		return nil, io.EOF
	}
	if n < 1 {
		return nil, core.Fail(core.InvalidRequest, nil)
	}
	limit := int32(min(n, 5000))
	var blobs []*container.BlobItem
	var prefixes []*container.BlobPrefix
	var next *string
	if i.delimiter == "" {
		if i.flat == nil {
			i.flat = i.d.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &i.prefix, MaxResults: &limit})
		}
		page, err := i.flat.NextPage(ctx)
		if err != nil {
			return nil, translate(err)
		}
		if page.Segment == nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		blobs, next = page.Segment.BlobItems, page.NextMarker
	} else {
		if i.hierarchy == nil {
			i.hierarchy = i.d.client.NewListBlobsHierarchyPager(i.delimiter, &container.ListBlobsHierarchyOptions{Prefix: &i.prefix, MaxResults: &limit})
		}
		page, err := i.hierarchy.NextPage(ctx)
		if err != nil {
			return nil, translate(err)
		}
		if page.Segment == nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		blobs, prefixes, next = page.Segment.BlobItems, page.Segment.BlobPrefixes, page.NextMarker
	}
	marker := ""
	if next != nil {
		marker = *next
	}
	if marker != "" && marker == i.marker {
		return nil, core.Fail(core.InvalidUpstreamResponse, errors.New("native continuation did not advance"))
	}
	items := make([]core.Metadata, 0, len(blobs)+len(prefixes))
	appendName := func(name *string, m core.Metadata) error {
		if name == nil || !strings.HasPrefix(*name, i.prefix) {
			return core.Fail(core.InvalidUpstreamResponse, nil)
		}
		m.Name = strings.TrimPrefix(*name, i.d.prefix)
		if m.Name == "" || !utf8.ValidString(m.Name) {
			return core.Fail(core.UnrepresentableName, nil)
		}
		if m.Name > i.after {
			items = append(items, m)
		}
		return nil
	}
	for _, b := range blobs {
		if b == nil || b.Properties == nil {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		p := b.Properties
		if p.ResourceType != nil && *p.ResourceType == "directory" {
			return nil, core.Fail(core.UnsupportedOperation, errors.New("hierarchical directories are not blob files"))
		}
		if err := appendName(b.Name, core.Metadata{Kind: core.File, Size: p.ContentLength, ModifiedAt: p.LastModified, Evidence: evidence(p.ETag, b.VersionID)}); err != nil {
			return nil, err
		}
	}
	for _, p := range prefixes {
		if p == nil || p.Name == nil || !strings.HasSuffix(*p.Name, i.delimiter) {
			return nil, core.Fail(core.InvalidUpstreamResponse, nil)
		}
		if err := appendName(p.Name, core.Metadata{Kind: core.PrefixGroup}); err != nil {
			return nil, err
		}
	}
	i.marker, i.done = marker, marker == ""
	if i.done {
		return items, io.EOF
	}
	return items, nil
}
