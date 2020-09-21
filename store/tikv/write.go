// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type actionWrite struct{}

var _ twoPhaseCommitAction = actionWrite{}

var tiKVTxnRegionsNumHistogramWrite = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("write"))

func (actionWrite) String() string {
	return "write"
}

func (actionWrite) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return tiKVTxnRegionsNumHistogramWrite
}

func (c *twoPhaseCommitter) buildWriteRequest(batch batchMutations) *tikvrpc.Request {
	m := &batch.mutations
	mutations := make([]*pb.Mutation, m.len())
	for i := range m.keys {
		mutations[i] = &pb.Mutation{
			Op:    m.ops[i],
			Key:   m.keys[i],
			Value: m.values[i],
		}
	}
	return tikvrpc.NewRequest(tikvrpc.CmdWrite, &pb.WriteRequest{
		Mutations: mutations,
		Version:   c.startTS,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
}

func (actionWrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	req := c.buildWriteRequest(batch)
	for {
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.writeMutations(bo, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		writeResp := resp.Resp.(*pb.WriteResponse)
		if reqErr := writeResp.GetError(); reqErr != "" {
			logutil.Logger(bo.ctx).Error("2PC failed to write", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
			return errors.New(reqErr)
		}
		return nil
	}
}

func (c *twoPhaseCommitter) writeMutations(bo *Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.writeMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	return c.doActionOnMutations(bo, actionWrite{}, mutations)
}
