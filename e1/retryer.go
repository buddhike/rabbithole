package main

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	metrics "github.com/influxdata/influxdb-client-go/v2/api"
)

type MetricRetryer struct {
	inner         aws.Retryer
	mu            *sync.Mutex
	retryCount    int
	metricsWriter metrics.WriteAPI
}

func (r *MetricRetryer) IsErrorRetryable(err error) bool {
	return r.inner.IsErrorRetryable(err)
}

func (r *MetricRetryer) MaxAttempts() int {
	return r.inner.MaxAttempts()
}

func (r *MetricRetryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	ir, err := r.inner.RetryDelay(attempt, opErr)
	if err == nil {
		r.mu.Lock()
		r.retryCount++
		c := r.retryCount
		r.mu.Unlock()
		r.metricsWriter.WritePoint(influxdb2.NewPointWithMeasurement("get-object-retryer").AddField("count", c))
	}
	return ir, err
}

func (r *MetricRetryer) GetRetryToken(ctx context.Context, opErr error) (func(error) error, error) {
	return r.inner.GetRetryToken(ctx, opErr)
}

func (r *MetricRetryer) GetInitialToken() func(error) error {
	return r.inner.GetInitialToken()
}

func NewMetricRetryer(mw metrics.WriteAPI) *MetricRetryer {
	return &MetricRetryer{
		inner:         retry.NewStandard(),
		mu:            &sync.Mutex{},
		metricsWriter: mw,
	}
}
