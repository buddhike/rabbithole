package main

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	metrics "github.com/influxdata/influxdb-client-go/v2/api"
)

type S3ClientProvider func() *s3.Client

type Client struct {
	id            int
	bucket        string
	key           string
	provider      S3ClientProvider
	wg            *sync.WaitGroup
	metricsWriter metrics.WriteAPI
}

func (c *Client) Go(ctx context.Context) {
	go func() {
		defer c.wg.Done()
		counter := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				start := time.Now()
				res, err := c.provider().GetObject(ctx, &s3.GetObjectInput{
					Bucket: &c.bucket,
					Key:    &c.key,
				})
				if err != nil {
					log.Printf("INFO: GetObject failed %v", err)
					if serr, ok := err.(smithy.APIError); ok {
						log.Printf("INFO: GetObject faild %v", serr.ErrorMessage())
						if serr.ErrorFault() == smithy.FaultClient {
							return
						}
					}
					continue
				}
				_, err = ioutil.ReadAll(res.Body)
				if err != nil {
					log.Printf("INFO: failed to read body: %v", err)
				}
				err = res.Body.Close()
				if err != nil {
					log.Printf("INFO: failed to close body: %v", err)
				}
				duration := time.Since(start)
				counter += 1
				c.metricsWriter.WritePoint(
					influxdb2.NewPointWithMeasurement("get-object").
						AddField("duration", duration.Milliseconds()).
						SetTime(time.Now()))
			}
		}
	}()
}

func NewClient(bucket, key string, provider S3ClientProvider, wg *sync.WaitGroup) *Client {
	return &Client{
		bucket:   bucket,
		key:      key,
		provider: provider,
		wg:       wg,
	}
}
