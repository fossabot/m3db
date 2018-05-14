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

package index

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	m3ninxindex "github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3ninx/search/executor"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
)

var (
	errUnableToWriteBlockClosed     = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed     = errors.New("unable to write, index block is sealed")
	errUnableToQueryBlockClosed     = errors.New("unable to query, index block is closed")
	errUnableToBootstrapBlockClosed = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed      = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed           = errors.New("unable to close, block already closed")

	errUnableToSealBlockIllegalStateFmtString  = "unable to seal, index block state: %v"
	errUnableToWriteBlockUnknownStateFmtString = "unable to write, unknown index block state: %v"
)

type blockState byte

const (
	blockStateClosed blockState = iota
	blockStateOpen
	blockStateSealed
)

type newExecutorFn func() (search.Executor, error)

type block struct {
	sync.RWMutex
	state                   blockState
	immutableSegments       []segment.Segment
	inactiveMutableSegments []segment.MutableSegment
	activeSegment           segment.MutableSegment

	newExecutorFn newExecutorFn
	startTime     time.Time
	endTime       time.Time
	blockSize     time.Duration
	opts          Options
	nsMD          namespace.Metadata
}

// NewBlock returns a new Block, representing a complete reverse index for the
// duration of time specified. It is backed by one or more segments.
func NewBlock(
	startTime time.Time,
	md namespace.Metadata,
	opts Options,
) (Block, error) {
	var (
		blockSize = md.Options().IndexOptions().BlockSize()
	)

	// FOLLOWUP(prateek): use this to track segments when we have multiple segments in a Block.
	postingsOffset := postings.ID(0)
	seg, err := mem.NewSegment(postingsOffset, opts.MemSegmentOptions())
	if err != nil {
		return nil, err
	}

	b := &block{
		state:         blockStateOpen,
		activeSegment: seg,

		startTime: startTime,
		endTime:   startTime.Add(blockSize),
		blockSize: blockSize,
		opts:      opts,
		nsMD:      md,
	}
	b.newExecutorFn = b.executorWithRLock

	return b, nil
}

func (b *block) StartTime() time.Time {
	return b.startTime
}

func (b *block) EndTime() time.Time {
	return b.endTime
}

func (b *block) WriteBatch(inserts *WriteBatch) (WriteBatchResult, error) {
	b.Lock()
	defer b.Unlock()

	if b.state != blockStateOpen {
		err := b.writeBatchErrorInvalidState(b.state)
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{
			NumError: int64(inserts.Len()),
		}, err
	}

	// NB: we're guaranteed the block (i.e. has a valid activeSegment) because
	// of the state check above. the if check below is additional paranoia.
	if b.activeSegment == nil { // should never happen
		err := b.openBlockHasNilActiveSegmentInvariantErrorWithRLock()
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{
			NumError: int64(inserts.Len()),
		}, err
	}

	err := b.activeSegment.InsertBatch(m3ninxindex.Batch{
		Docs:                inserts.PendingDocs(),
		AllowPartialUpdates: true,
	})
	if err == nil {
		inserts.MarkUnmarkedEntriesSuccess()
		return WriteBatchResult{
			NumSuccess: int64(inserts.Len()),
		}, nil
	}

	partialErr, ok := err.(*m3ninxindex.BatchPartialError)
	if !ok { // should never happen
		err := b.unknownWriteBatchInvariantError(err)
		// NB: marking all the inserts as failure, cause we don't know which ones failed
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{NumError: int64(inserts.Len())}, err
	}

	numErr := len(partialErr.Errs())
	for _, err := range partialErr.Errs() {
		// Avoid marking these as success
		inserts.MarkUnmarkedEntryError(err.Err, err.Idx)
	}

	// mark all non-error inserts success, so we don't repeatedly index them
	inserts.MarkUnmarkedEntriesSuccess()
	return WriteBatchResult{
		NumSuccess: int64(inserts.Len() - numErr),
		NumError:   int64(numErr),
	}, partialErr
}

func (b *block) executorWithRLock() (search.Executor, error) {
	var (
		readers = make([]m3ninxindex.Reader, 0, 1+len(b.inactiveMutableSegments)+len(b.immutableSegments))
		success = false
	)

	// cleanup in case any of the readers below fail.
	defer func() {
		if !success {
			for _, reader := range readers {
				reader.Close()
			}
		}
	}()

	// start with the segment that's being actively written to (if any)
	// NB: need the nil check because we could have evicted this segment to disk already
	if b.activeSegment != nil {
		reader, err := b.activeSegment.Reader()
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}

	// include all immutable segments
	for _, seg := range b.immutableSegments {
		reader, err := seg.Reader()
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}

	// include all inactiveMutable segments
	for _, seg := range b.inactiveMutableSegments {
		reader, err := seg.Reader()
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}

	success = true
	return executor.NewExecutor(readers), nil
}

func (b *block) Query(
	query Query,
	opts QueryOptions,
	results Results,
) (bool, error) {
	b.RLock()
	defer b.RUnlock()
	if b.state == blockStateClosed {
		return false, errUnableToQueryBlockClosed
	}

	exec, err := b.newExecutorFn()
	if err != nil {
		return false, err
	}

	// FOLLOWUP(prateek): push down QueryOptions to restrict results
	// TODO(jeromefroe): Use the idx query directly once we implement an index in m3ninx
	// and don't need to use the segments anymore.
	iter, err := exec.Execute(query.Query.SearchQuery())
	if err != nil {
		exec.Close()
		return false, err
	}

	var (
		size       = results.Size()
		brokeEarly = false
	)
	execCloser := safeCloser{closable: exec}
	iterCloser := safeCloser{closable: iter}

	defer func() {
		iterCloser.Close()
		execCloser.Close()
	}()

	for iter.Next() {
		if opts.Limit > 0 && size >= opts.Limit {
			brokeEarly = true
			break
		}
		d := iter.Current()
		_, size, err = results.Add(d)
		if err != nil {
			return false, err
		}
	}

	if err := iter.Err(); err != nil {
		return false, err
	}

	if err := iterCloser.Close(); err != nil {
		return false, err
	}

	if err := execCloser.Close(); err != nil {
		return false, err
	}

	exhaustive := !brokeEarly
	return exhaustive, nil
}

func (b *block) Bootstrap(
	segments []segment.Segment,
) error {
	b.Lock()
	defer b.Unlock()

	// NB(prateek): we have to allow bootstrap to succeed even if we're Sealed because
	// of topology changes. i.e. if the current m3db process is assigned new shards,
	// we need to include their data in the index.

	// i.e. the only state we do not accept bootstrapped data is if we are closed.
	if b.state == blockStateClosed {
		return errUnableToBootstrapBlockClosed
	}

	// NB: need to check if the current block has been marked 'Sealed' and if so,
	// mark all incoming mutable segments the same.
	isSealed := b.IsSealedWithRLock()

	var multiErr xerrors.MultiError
	for _, seg := range segments {
		switch x := seg.(type) {
		case segment.MutableSegment:
			if isSealed {
				_, err := x.Seal()
				if err != nil {
					// if this happens it means a Mutable segment was marked sealed
					// in the bootstrappers, this should never happen.
					multiErr = multiErr.Add(b.bootstrappingSealedMutableSegmentInvariant(err))
				}
			}
			b.inactiveMutableSegments = append(b.inactiveMutableSegments, x)
		default:
			b.immutableSegments = append(b.immutableSegments, x)
		}
	}

	return multiErr.FinalError()
}

func (b *block) Flush(flush persist.IndexFlush) error {
	b.RLock()
	defer b.RUnlock()

	// we only flush if the block has been marked sealed.
	if b.state != blockStateSealed {
		return fmt.Errorf("unable to flush block in state: %v", b.state)
	}

	// NB: it is safe to do this with an RLock as the current block has been marked sealed
	// and is not accepting any incoming writes. We don't acquire a write lock until we
	// have finished persisting data to allow reads to succeed while we are flushing.

	// as index fileset files are allowed to have multiple volumes, each block tracks
	// whether it needs to be flushed internally based on the presence of mutable state.
	// if we don't have any such state, we do not need to flush.
	needsFlush := b.activeSegment != nil || len(b.inactiveMutableSegments) != 0
	if !needsFlush {
		return nil
	}

	// prepare the persistence object now that we're sure we need to write.
	prepared, err := flush.PrepareIndex(persist.IndexPrepareOptions{
		BlockStart:        b.startTime,
		FileSetType:       persist.FileSetFlushType,
		NamespaceMetadata: b.nsMD,
	})
	if err != nil {
		return err
	}
	preparedClosed := false
	defer func() {
		if !preparedClosed {
			prepared.Close()
		}
	}()

	// flush all held mutable segments
	numSegmentsFlushed := 0 // TODO(prateek): emit a metric for this.

	// NB: we deliberately choose to avoid merging mutable segments to avoid extra allocations
	// during flush.

	// check if we have an active MutableSegment (we might not, if we have already flushed it).
	if b.activeSegment != nil {
		if err := prepared.Persist(b.activeSegment); err != nil {
			return err
		}
		numSegmentsFlushed++
	}

	// flush all inactive mutable segments
	for _, seg := range b.inactiveMutableSegments {
		if err := prepared.Persist(seg); err != nil {
			return err
		}
		numSegmentsFlushed++
	}

	// indicate that the defer above does not need to Close() `prepared`, as we
	// are closing it ourselves.
	preparedClosed = true

	// retrieve persisted versions of the segments we just flushed so we can evict
	// the mutable segments from memory.
	persistedSegments, err := prepared.Close()
	if err != nil {
		return err
	}

	// swap mutable segments with the persisted segments
	b.replaceMutableSegmentsWithRLock(persistedSegments)

	// all good!
	return nil
}

func (b *block) replaceMutableSegmentsWithRLock(persistedSegments []segment.Segment) {
	// NB: need to do this RLock -> Lock -> RLock swap as we enter this
	// function with a RLock; need to mutate some state, and then exit this
	// function with the RLock so the caller still holds a RLock.
	b.RUnlock()
	b.Lock()
	defer func() {
		b.Unlock()
		b.RLock()
	}()

	// update immutable segments held onto by the block.
	b.immutableSegments = append(b.immutableSegments, persistedSegments...)
	var multiErr xerrors.MultiError

	// invalidate active mutable segment
	if b.activeSegment != nil {
		multiErr = multiErr.Add(b.activeSegment.Close())
		b.activeSegment = nil
	}

	// invalidate inactive mutable segments
	for idx := range b.inactiveMutableSegments {
		multiErr = multiErr.Add(b.inactiveMutableSegments[idx].Close())
		b.inactiveMutableSegments[idx] = nil
	}
	b.inactiveMutableSegments = b.inactiveMutableSegments[:0]

	if multiErr.Empty() {
		return
	}

	// we deliberately choose to skip returns any errors here, as the persisted
	// segments have all the state present in the mutable segments, reporting
	// this as a failure buys us nothing.
	logger := b.opts.InstrumentOptions().Logger()
	logger.Warnf("encountered errors while closing mutable segments while replacing with persistent segments: %v",
		multiErr.FinalError())
}

func (b *block) Tick(c context.Cancellable) (BlockTickResult, error) {
	b.RLock()
	defer b.RUnlock()
	result := BlockTickResult{}
	if b.state == blockStateClosed {
		return result, errUnableToTickBlockClosed
	}

	// active segment, can be nil incase we've evicted it already.
	if b.activeSegment != nil {
		result.NumSegments++
		result.NumDocs += b.activeSegment.Size()
	}

	// any other mutable segments
	for _, seg := range b.inactiveMutableSegments {
		result.NumSegments++
		result.NumDocs += seg.Size()
	}

	// any immutable segments
	for _, seg := range b.immutableSegments {
		result.NumSegments++
		result.NumDocs += seg.Size()
	}

	return result, nil
}

func (b *block) Seal() error {
	b.Lock()
	defer b.Unlock()

	// ensure we only Seal if we're marked Open
	if b.state != blockStateOpen {
		return fmt.Errorf(errUnableToSealBlockIllegalStateFmtString, b.state)
	}
	b.state = blockStateSealed

	var multiErr xerrors.MultiError

	// seal active mutable segment.
	if b.activeSegment != nil {
		_, err := b.activeSegment.Seal()
		multiErr = multiErr.Add(err)
	} else {
		// should never happen, as we only mark the active mutable segment
		// sealed within this function, and block.Seal() should only be called once.
		multiErr = multiErr.Add(b.nilActiveSegmentWhileSealingInvariant())
	}

	// seal any inactive mutable segments.
	for _, seg := range b.inactiveMutableSegments {
		_, err := seg.Seal()
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}

func (b *block) IsSealedWithRLock() bool {
	return b.state == blockStateSealed
}

func (b *block) IsSealed() bool {
	b.RLock()
	defer b.RUnlock()
	return b.IsSealedWithRLock()
}

func (b *block) Close() error {
	b.Lock()
	defer b.Unlock()
	if b.state == blockStateClosed {
		return errBlockAlreadyClosed
	}
	b.state = blockStateClosed

	var multiErr xerrors.MultiError

	// we may not have an active mutable segment if it's already
	// been flushed.
	if b.activeSegment != nil {
		multiErr = multiErr.Add(b.activeSegment.Close())
	}
	b.activeSegment = nil

	// close any inactiveMutable segments.
	for _, seg := range b.inactiveMutableSegments {
		multiErr = multiErr.Add(seg.Close())
	}
	b.inactiveMutableSegments = nil

	// close all immutable segments.
	for _, seg := range b.immutableSegments {
		multiErr = multiErr.Add(seg.Close())
	}
	b.immutableSegments = nil

	return multiErr.FinalError()
}

func (b *block) writeBatchErrorInvalidState(state blockState) error {
	switch state {
	case blockStateClosed:
		return errUnableToWriteBlockClosed
	case blockStateSealed:
		return errUnableToWriteBlockSealed
	default: // should never happen
		err := fmt.Errorf(errUnableToWriteBlockUnknownStateFmtString, state)
		instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(err.Error())
		return err
	}
}

func (b *block) openBlockHasNilActiveSegmentInvariantErrorWithRLock() error {
	err := fmt.Errorf("internal error: block has open block state [%v] has nil active segment", b.state)
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(err.Error())
	return err
}

func (b *block) unknownWriteBatchInvariantError(err error) error {
	wrappedErr := fmt.Errorf("unexpected non-BatchPartialError from m3ninx InsertBatch: %v", err)
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(wrappedErr.Error())
	return wrappedErr
}

func (b *block) bootstrappingSealedMutableSegmentInvariant(err error) error {
	wrapped := fmt.Errorf("internal error: bootstrapping a mutable segment already marked sealed: %v", err)
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(wrapped.Error())
	return wrapped
}

func (b *block) nilActiveSegmentWhileSealingInvariant() error {
	err := fmt.Errorf("internal error: found nil active segment while sealing index block")
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(err.Error())
	return err
}

type closable interface {
	Close() error
}

type safeCloser struct {
	closable
	closed bool
}

func (c *safeCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.closable.Close()
}
