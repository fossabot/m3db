// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package namespace

import (
	"errors"
	"time"

	nsproto "github.com/m3db/m3db/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

var (
	errRetentionNil = errors.New("retention options must be set")
	errNamespaceNil = errors.New("namespace options must be set")
)

func fromNanos(n int64) time.Duration {
	return xtime.FromNormalizedDuration(n, time.Nanosecond)
}

// ToRetention converts nsproto.RetentionOptions to retention.Options
func ToRetention(
	ro *nsproto.RetentionOptions,
) (retention.Options, error) {
	if ro == nil {
		return nil, errRetentionNil
	}

	ropts := retention.NewOptions().
		SetRetentionPeriod(fromNanos(ro.RetentionPeriodNanos)).
		SetBlockSize(fromNanos(ro.BlockSizeNanos)).
		SetBufferFuture(fromNanos(ro.BufferFutureNanos)).
		SetBufferPast(fromNanos(ro.BufferPastNanos)).
		SetBlockDataExpiry(ro.BlockDataExpiry).
		SetBlockDataExpiryAfterNotAccessedPeriod(
			fromNanos(ro.BlockDataExpiryAfterNotAccessPeriodNanos))

	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	return ropts, nil
}

// ToIndexOptions converts nsproto.IndexOptions to IndexOptions
func ToIndexOptions(
	io *nsproto.IndexOptions,
) (IndexOptions, error) {
	iopts := NewIndexOptions().SetEnabled(false)
	if io == nil {
		return iopts, nil
	}

	iopts = iopts.SetEnabled(io.Enabled).
		SetBlockSize(fromNanos(io.BlockSizeNanos))

	return iopts, nil
}

// ToMetadata converts nsproto.Options to Metadata
func ToMetadata(
	id string,
	opts *nsproto.NamespaceOptions,
) (Metadata, error) {
	if opts == nil {
		return nil, errNamespaceNil
	}

	ropts, err := ToRetention(opts.RetentionOptions)
	if err != nil {
		return nil, err
	}

	iopts, err := ToIndexOptions(opts.IndexOptions)
	if err != nil {
		return nil, err
	}

	mopts := NewOptions().
		SetBootstrapEnabled(opts.BootstrapEnabled).
		SetFlushEnabled(opts.FlushEnabled).
		SetCleanupEnabled(opts.CleanupEnabled).
		SetRepairEnabled(opts.RepairEnabled).
		SetWritesToCommitLog(opts.WritesToCommitLog).
		SetSnapshotEnabled(opts.SnapshotEnabled).
		SetRetentionOptions(ropts).
		SetIndexOptions(iopts)

	return NewMetadata(ident.StringID(id), mopts)
}

// ToProto converts Map to nsproto.Registry
func ToProto(m Map) *nsproto.Registry {
	reg := nsproto.Registry{
		Namespaces: make(map[string]*nsproto.NamespaceOptions, len(m.Metadatas())),
	}

	toNanos := func(t time.Duration) int64 {
		return xtime.ToNormalizedDuration(t, time.Nanosecond)
	}

	for _, md := range m.Metadatas() {
		ropts := md.Options().RetentionOptions()
		iopts := md.Options().IndexOptions()
		reg.Namespaces[md.ID().String()] = &nsproto.NamespaceOptions{
			BootstrapEnabled:  md.Options().BootstrapEnabled(),
			FlushEnabled:      md.Options().FlushEnabled(),
			CleanupEnabled:    md.Options().CleanupEnabled(),
			SnapshotEnabled:   md.Options().SnapshotEnabled(),
			RepairEnabled:     md.Options().RepairEnabled(),
			WritesToCommitLog: md.Options().WritesToCommitLog(),
			RetentionOptions: &nsproto.RetentionOptions{
				BlockSizeNanos:                           toNanos(ropts.BlockSize()),
				RetentionPeriodNanos:                     toNanos(ropts.RetentionPeriod()),
				BufferFutureNanos:                        toNanos(ropts.BufferFuture()),
				BufferPastNanos:                          toNanos(ropts.BufferPast()),
				BlockDataExpiry:                          ropts.BlockDataExpiry(),
				BlockDataExpiryAfterNotAccessPeriodNanos: toNanos(ropts.BlockDataExpiryAfterNotAccessedPeriod()),
			},
			IndexOptions: &nsproto.IndexOptions{
				Enabled:        iopts.Enabled(),
				BlockSizeNanos: toNanos(iopts.BlockSize()),
			},
		}
	}

	return &reg
}

// FromProto converts nsproto.Registry -> Map
func FromProto(protoRegistry nsproto.Registry) (Map, error) {
	metadatas := make([]Metadata, 0, len(protoRegistry.Namespaces))
	for ns, opts := range protoRegistry.Namespaces {
		md, err := ToMetadata(ns, opts)
		if err != nil {
			return nil, err
		}
		metadatas = append(metadatas, md)
	}
	return NewMap(metadatas)
}
