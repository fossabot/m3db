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

package fs

import (
	"fmt"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
)

type dataWriterOpenOptionsMatcher struct {
	DataWriterOpenOptions
}

var _ gomock.Matcher = dataWriterOpenOptionsMatcher{}

func (m dataWriterOpenOptionsMatcher) Matches(x interface{}) bool {
	obs, ok := x.(DataWriterOpenOptions)
	if !ok {
		return false
	}
	return cmp.Equal(m.DataWriterOpenOptions, obs)
}

func (m dataWriterOpenOptionsMatcher) String() string {
	return fmt.Sprintf("%v", m.DataWriterOpenOptions)
}

type indexWriterOpenOptionsMatcher struct {
	IndexWriterOpenOptions
}

var _ gomock.Matcher = indexWriterOpenOptionsMatcher{}

func (io indexWriterOpenOptionsMatcher) Matches(x interface{}) bool {
	opts, ok := x.(IndexWriterOpenOptions)
	if !ok {
		return false
	}
	return cmp.Equal(io.IndexWriterOpenOptions, opts)
}

func (io indexWriterOpenOptionsMatcher) String() string {
	return fmt.Sprintf("%v", io.IndexWriterOpenOptions)
}

type indexReaderOpenOptionsMatcher struct {
	IndexReaderOpenOptions
}

var _ gomock.Matcher = indexReaderOpenOptionsMatcher{}

func (io indexReaderOpenOptionsMatcher) Matches(x interface{}) bool {
	opts, ok := x.(IndexReaderOpenOptions)
	if !ok {
		return false
	}
	return cmp.Equal(io.IndexReaderOpenOptions, opts)
}

func (io indexReaderOpenOptionsMatcher) String() string {
	return fmt.Sprintf("%v", io.IndexReaderOpenOptions)
}

// FOLLOWUP(prateek): wire up a reflection based gomock.Matcher using go-cmp
// this method of creating one per type is terrible.
