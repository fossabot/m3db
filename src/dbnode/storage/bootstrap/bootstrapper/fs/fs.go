// Copyright (c) 2016 Uber Technologies, Inc.
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
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper"
)

const (
	// FileSystemBootstrapperName is the name of th filesystem bootstrapper.
	FileSystemBootstrapperName = "filesystem"
)

type fileSystemBootstrapperProvider struct {
	opts Options
	next bootstrap.BootstrapperProvider
}

// NewFileSystemBootstrapperProvider creates a new bootstrapper to bootstrap from on-disk files.
func NewFileSystemBootstrapperProvider(
	opts Options,
	next bootstrap.BootstrapperProvider,
) bootstrap.BootstrapperProvider {
	return fileSystemBootstrapperProvider{
		opts: opts,
		next: next,
	}
}

func (p fileSystemBootstrapperProvider) Provide() bootstrap.Bootstrapper {
	var (
		src  = newFileSystemSource(p.opts)
		b    = &fileSystemBootstrapper{}
		next bootstrap.Bootstrapper
	)
	if p.next != nil {
		next = p.next.Provide()
	}
	b.Bootstrapper = bootstrapper.NewBaseBootstrapper(b.String(),
		src, p.opts.ResultOptions(), next)
	return b
}

func (p fileSystemBootstrapperProvider) String() string {
	return FileSystemBootstrapperName
}

type fileSystemBootstrapper struct {
	bootstrap.Bootstrapper
}

func (fsb *fileSystemBootstrapper) String() string {
	return FileSystemBootstrapperName
}
