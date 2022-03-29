// https://raw.githubusercontent.com/prometheus/prometheus/3ac96c7841ed2d81a9611bd3c158007a85559c98/tsdb/importer/blocks/writer.go
// Copyright 2020 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blocks

import (
	"context"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Writer is interface to write time series into Prometheus blocks.
type Writer interface {
	storage.Appendable

	// Flush writes current data to disk.
	// The block or blocks will contain values accumulated by `Write`.
	Flush() ([]ulid.ULID, error)

	// Close releases all resources. No append is allowed anymore to such writer.
	Close() error
}

var _ Writer = &TSDBWriter{}

// Writer is a block writer that allows appending and flushing to disk.
type TSDBWriter struct {
	logger log.Logger
	dir    string
	labels labels.Labels

	head   *tsdb.Head
	tmpDir string
}

func DurToMillis(t time.Duration) int64 {
	return int64(t.Seconds() * 1000)
}

// NewTSDBWriter create new block writer.
//
// The returned writer accumulates all series in memory until `Flush` is called.
//
// Note that the writer will not check if the target directory exists or
// contains anything at all. It is the caller's responsibility to
// ensure that the resulting blocks do not overlap etc.
// Writer ensures the block flush is atomic (via rename).
func NewTSDBWriter(logger log.Logger, dir string, labels labels.Labels) (*TSDBWriter, error) {
	res := &TSDBWriter{
		logger: logger,
		dir:    dir,
		labels: labels,
	}
	return res, res.initHead()
}

// initHead creates and initialises new head.
func (w *TSDBWriter) initHead() error {
	logger := w.logger

	// Keep Registerer and WAL nil as we don't use them.
	// Put huge chunkRange; It has to be equal then expected block size.
	// Since we don't have info about block size here, set it to large number.

	tmpDir, err := ioutil.TempDir(os.TempDir(), "head")
	if err != nil {
		return errors.Wrap(err, "create temp dir")
	}
	w.tmpDir = tmpDir
	opts := tsdb.DefaultHeadOptions()
	opts.ChunkRange = DurToMillis(9999 * time.Hour)
	opts.ChunkDirRoot = w.tmpDir
	h, err := tsdb.NewHead(nil, logger, nil, opts, tsdb.NewHeadStats())
	if err != nil {
		return errors.Wrap(err, "tsdb.NewHead")
	}

	w.head = h
	return w.head.Init(math.MinInt64)
}

// Appender is not thread-safe. Returned Appender is thread-save however.
func (w *TSDBWriter) Appender(ctx context.Context) storage.Appender {
	return w.head.Appender(ctx)
}

// Flush implements Writer interface. This is where actual block writing
// happens. After flush completes, no write can be done.
func (w *TSDBWriter) Flush() ([]ulid.ULID, error) {
	seriesCount := w.head.NumSeries()
	if w.head.NumSeries() == 0 {
		return nil, errors.New("no series appended; aborting.")
	}

	mint := w.head.MinTime()
	maxt := w.head.MaxTime() + 1
	level.Info(w.logger).Log("msg", "flushing", "series_count", seriesCount, "mint", timestamp.Time(mint), "maxt", timestamp.Time(maxt))

	// Flush head to disk as a block.
	compactor, err := tsdb.NewLeveledCompactor(
		context.Background(),
		nil,
		w.logger,
		[]int64{DurToMillis(2 * time.Hour)}, // Does not matter, used only for planning.
		chunkenc.NewPool(),
		nil)
	if err != nil {
		return nil, errors.Wrap(err, "create leveled compactor")
	}
	id, err := compactor.Write(w.dir, w.head, mint, maxt, nil)
	if err != nil {
		return nil, errors.Wrap(err, "compactor write")
	}
	// TODO(bwplotka): Potential truncate head, and allow writer reuse. Currently truncating fails with
	// truncate chunks.HeadReadWriter: maxt of the files are not set.

	meta, err := metadata.ReadFromDir(filepath.Join(w.dir, id.String()))
	if err != nil {
		return nil, errors.Wrap(err, "metadata read")
	}
	meta.Thanos.Source = "thanos-kit"
	meta.Thanos.Labels = w.labels.Map()
	if err = meta.WriteToDir(w.logger, filepath.Join(w.dir, id.String())); err != nil {
		return nil, errors.Wrap(err, "metadata write")
	}
	return []ulid.ULID{id}, nil
}

func (w *TSDBWriter) Close() error {
	_ = os.RemoveAll(w.tmpDir)
	return w.head.Close()
}
