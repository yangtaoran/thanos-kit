// https://github.com/prometheus/prometheus/blob/3ac96c7841ed2d81a9611bd3c158007a85559c98/tsdb/importer/import.go
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

package importer

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/storage"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/yangtaoran/thanos-kit/importer/blocks"
)

// Import imports data from a textparse Parser into block Writer.
// TODO(bwplotka): textparse interface potentially limits the format to never give multiple samples. Fix this as some formats
// (e.g JSON) might allow that.
// Import takes ownership of given block writer.
func Import(logger log.Logger, p textparse.Parser, w blocks.Writer) (ids []ulid.ULID, err error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	level.Info(logger).Log("msg", "started importing input data.")
	app := w.Appender(context.Background())

	defer func() {
		merr := tsdb_errors.NewMulti()
		merr.Add(err)
		merr.Add(w.Close())
		err = merr.Err()
	}()

	var (
		e   textparse.Entry
		ref storage.SeriesRef
	)
	for {
		e, err = p.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "parse")
		}

		// For now care about series only.
		if e != textparse.EntrySeries {
			continue
		}

		// TODO(bwplotka): Avoid allocations using AddFast method and maintaining refs.
		l := labels.Labels{}
		p.Metric(&l)
		_, ts, v := p.Series()
		if ts == nil {
			return nil, errors.Errorf("expected timestamp for series %v, got none", l.String())
		}
		if ref, err = app.Append(ref, l, *ts, v); err != nil {
			return nil, errors.Wrap(err, "add sample")
		}
	}

	level.Info(logger).Log("msg", "no more input data, committing appenders and flushing block(s)")
	if err := app.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit")
	}

	ids, err = w.Flush()
	if err != nil {
		return nil, errors.Wrap(err, "flush")
	}
	level.Info(logger).Log("msg", "blocks flushed", "ids", fmt.Sprintf("%v", ids))
	return ids, nil
}
