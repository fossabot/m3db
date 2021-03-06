// Copyright (c) 2018 Uber Technologies, Inc.
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

package result

import (
	"time"

	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3ninx/index/segment"
	xtime "github.com/m3db/m3x/time"
)

type indexBootstrapResult struct {
	results     IndexResults
	unfulfilled ShardTimeRanges
}

// NewIndexBootstrapResult returns a new index bootstrap result.
func NewIndexBootstrapResult() IndexBootstrapResult {
	return &indexBootstrapResult{
		results:     make(IndexResults),
		unfulfilled: make(ShardTimeRanges),
	}
}

func (r *indexBootstrapResult) IndexResults() IndexResults {
	return r.results
}

func (r *indexBootstrapResult) Unfulfilled() ShardTimeRanges {
	return r.unfulfilled
}

func (r *indexBootstrapResult) SetUnfulfilled(unfulfilled ShardTimeRanges) {
	r.unfulfilled = unfulfilled
}

func (r *indexBootstrapResult) Add(block IndexBlock, unfulfilled ShardTimeRanges) {
	r.results.Add(block)
	r.unfulfilled.AddRanges(unfulfilled)
}

// Add will add an index block to the collection, merging if one already
// exists.
func (r IndexResults) Add(block IndexBlock) {
	if block.BlockStart().IsZero() {
		return
	}

	// Merge results
	blockStart := xtime.ToUnixNano(block.BlockStart())
	existing, ok := r[blockStart]
	if !ok {
		r[blockStart] = block
		return
	}
	r[blockStart] = existing.Merged(block)
}

// AddResults will add another set of index results to the collection, merging
// if index blocks already exists.
func (r IndexResults) AddResults(other IndexResults) {
	for _, block := range other {
		r.Add(block)
	}
}

// GetOrAddSegment get or create a new mutable segment.
func (r IndexResults) GetOrAddSegment(
	t time.Time,
	idxopts namespace.IndexOptions,
	opts Options,
) (segment.MutableSegment, error) {
	// NB(r): The reason we can align by the retention block size and guarantee
	// there is only one entry for this time is because index blocks must be a
	// positive multiple of the data block size, making it easy to map a data
	// block entry to at most one index block entry.
	blockStart := t.Truncate(idxopts.BlockSize())
	blockStartNanos := xtime.ToUnixNano(blockStart)

	block, exists := r[blockStartNanos]
	if !exists {
		block = NewIndexBlock(blockStart, nil)
		r[blockStartNanos] = block
	}
	for _, seg := range block.Segments() {
		if mutable, ok := seg.(segment.MutableSegment); ok {
			return mutable, nil
		}
	}

	alloc := opts.IndexMutableSegmentAllocator()
	mutable, err := alloc()
	if err != nil {
		return nil, err
	}

	segments := []segment.Segment{mutable}
	r[blockStartNanos] = block.Merged(NewIndexBlock(blockStart, segments))
	return mutable, nil
}

// MergedIndexBootstrapResult returns a merged result of two bootstrap results.
// It is a mutating function that mutates the larger result by adding the
// smaller result to it and then finally returns the mutated result.
func MergedIndexBootstrapResult(i, j IndexBootstrapResult) IndexBootstrapResult {
	if i == nil {
		return j
	}
	if j == nil {
		return i
	}
	sizeI, sizeJ := 0, 0
	for _, ir := range i.IndexResults() {
		sizeI += len(ir.Segments())
	}
	for _, ir := range j.IndexResults() {
		sizeJ += len(ir.Segments())
	}
	if sizeI >= sizeJ {
		i.IndexResults().AddResults(j.IndexResults())
		i.Unfulfilled().AddRanges(j.Unfulfilled())
		return i
	}
	j.IndexResults().AddResults(i.IndexResults())
	j.Unfulfilled().AddRanges(i.Unfulfilled())
	return j
}

// NewIndexBlock returns a new bootstrap index block result.
func NewIndexBlock(
	blockStart time.Time,
	segments []segment.Segment,
) IndexBlock {
	return IndexBlock{
		blockStart: blockStart,
		segments:   segments,
	}
}

// BlockStart returns the block start.
func (b IndexBlock) BlockStart() time.Time {
	return b.blockStart
}

// Segments returns the segments.
func (b IndexBlock) Segments() []segment.Segment {
	return b.segments
}

// Merged returns a new merged index block, currently it just appends the
// list of segments from the other index block and the caller merges
// as they see necessary.
func (b IndexBlock) Merged(other IndexBlock) IndexBlock {
	r := b
	r.segments = append(r.segments, other.segments...)
	return r
}
