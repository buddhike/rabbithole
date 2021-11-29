package main

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	metrics "github.com/influxdata/influxdb-client-go/v2/api"
)

type ClientV1 struct {
	id            int
	bucket        string
	key           string
	wg            *sync.WaitGroup
	metricsWriter metrics.WriteAPI
	s3            *s3.S3
}

func (c *ClientV1) Go(ctx context.Context) {
	go func() {
		defer c.wg.Done()
		count := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				start := time.Now()
				rctx, cancel := context.WithCancel(ctx)
				res, err := c.s3.GetObjectWithContext(rctx, &s3.GetObjectInput{
					Bucket: &c.bucket,
					Key:    &c.key,
				})
				if err == nil {
					_, err = io.Copy(ioutil.Discard, res.Body)
					if err != nil {
						log.Printf("INFO: failed to read body: %v", err)
					}
					err = res.Body.Close()
					if err != nil {
						log.Printf("INFO: failed to close body: %v", err)
					}
					duration := time.Since(start)
					cancel()
					c.metricsWriter.WritePoint(
						influxdb2.NewPointWithMeasurement("get-object").
							AddField("duration", duration.Milliseconds()).
							SetTime(time.Now()))
				} else {
					cancel()
					log.Printf("INFO: GetObject failed %v", err)
					if aerr, ok := err.(awserr.Error); ok && (aerr.Code() == s3.ErrCodeNoSuchBucket || aerr.Code() == s3.ErrCodeNoSuchKey) {
						return
					}
				}
				count += 1
				log.Printf("count: %d", count)
			}
		}
	}()
}

func NewClientV1(id int, bucket, key string, wg *sync.WaitGroup, mw metrics.WriteAPI, maxIdleConns int) *ClientV1 {
	t := *(http.DefaultTransport.(*http.Transport))
	if maxIdleConns != 0 {
		t.MaxIdleConnsPerHost = maxIdleConns
	}
	tt := NewTracingRoundTripper(&t, mw)
	c := &http.Client{Transport: tt}
	sess := session.Must(session.NewSession(&aws.Config{
		HTTPClient: c,
	}))
	svc := s3.New(sess)

	return &ClientV1{
		id:            id,
		bucket:        bucket,
		key:           key,
		wg:            wg,
		metricsWriter: mw,
		s3:            svc,
	}
}
