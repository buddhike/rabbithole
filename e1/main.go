package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	metrics "github.com/influxdata/influxdb-client-go/v2/api"
)

type options struct {
	duration            string
	concurrency         int
	s3Path              string
	reuseConfig         bool
	reuseClient         bool
	generateObjects     bool
	numberOfTestObjects int
	maxIdleConns        int
}

var opts *options = &options{}

func init() {
	flag.StringVar(&opts.duration, "duration", "10s", "duration of test")
	flag.IntVar(&opts.concurrency, "concurrency", 1, "number of concurrenct requests to simulate")
	flag.StringVar(&opts.s3Path, "s3-path", "", "s3 path with test files")
	flag.BoolVar(&opts.reuseConfig, "reuse-config", false, "reuse aws config")
	flag.BoolVar(&opts.reuseClient, "reuse-client", false, "reuse aws client")
	flag.IntVar(&opts.maxIdleConns, "max-idle-conns", 0, "adjust MaxIdleConns setting in http.Transport")
	flag.BoolVar(&opts.generateObjects, "generate-objects", false, "generate test objects in s3")
	flag.IntVar(&opts.numberOfTestObjects, "test-object-count", 1000, "number of test objects to generate")
}

func getCloudwatchClient() *cloudwatch.Client {
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	return cloudwatch.NewFromConfig(config)
}

func createConfig(retryer aws.Retryer) aws.Config {
	var (
		cfg aws.Config
		err error
	)
	if opts.maxIdleConns == 0 {
		cfg, err = config.LoadDefaultConfig(context.TODO())
	} else {
		transport := *(http.DefaultTransport.(*http.Transport))
		transport.MaxIdleConnsPerHost = opts.maxIdleConns
		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithHTTPClient(&http.Client{Transport: &transport}), config.WithRetryer(func() aws.Retryer {
			return retryer
		}))
	}
	if err != nil {
		panic(err)
	}
	return cfg
}

func getS3Client(retryer aws.Retryer) func() *s3.Client {
	var (
		cfg    aws.Config
		client *s3.Client
	)

	cfg = createConfig(retryer)
	if cfg.HTTPClient != nil {
		hc := cfg.HTTPClient.(*http.Client)
		transport := hc.Transport.(*http.Transport)
		fmt.Printf("DisableCompression: %v\nDisableKeepAlives: %v\nExpectContinueTimeout: %v\nForceAttemptHTTP2: %v\nIdleConnTimeout: %v\nMaxConnsPerHost: %v\nMaxIdleConns: %v\nMaxIdleConnsPerHost: %v\nMaxResponseHeaderBytes: %v\nReadBufferSize: %d\nResponseHeaderTimeout: %v\nTLSHandshakeTimeout: %v\nTLSTransportMinVersion: %v\n", transport.DisableCompression, transport.DisableKeepAlives, transport.ExpectContinueTimeout, transport.ForceAttemptHTTP2, transport.IdleConnTimeout, transport.MaxConnsPerHost, transport.MaxIdleConns, transport.MaxIdleConnsPerHost, transport.MaxResponseHeaderBytes, transport.ReadBufferSize, transport.ResponseHeaderTimeout, transport.TLSHandshakeTimeout, transport.TLSClientConfig.MinVersion)
	}
	client = s3.NewFromConfig(cfg)
	return func() *s3.Client {
		if opts.reuseClient {
			return client
		}
		if opts.reuseConfig {
			return s3.NewFromConfig(cfg)
		}
		return s3.NewFromConfig(createConfig(retryer))
	}
}

func generateTestObjects(bucket, prefix string) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	c := s3.NewFromConfig(cfg)
	emptyBuf := make([]byte, 1024)
	for i := 0; i < opts.numberOfTestObjects; i++ {
		_, err := c.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("%s/%d/%d.obj", prefix, i, i)),
			Body:   bytes.NewBuffer(emptyBuf),
		})
		if err != nil {
			panic(err)
		}
	}
}

func decodeBucketParts(p string) (bucket, key string) {
	parts := strings.Split(p, "/")
	bucket = parts[0]
	key = strings.Join(parts[1:], "/")
	return
}

func createMetricWriter() (metrics.WriteAPI, func()) {
	var (
		ok     bool
		url    string
		token  string
		org    string
		bucket string
	)
	if url, ok = os.LookupEnv("INFLUX_URL"); !ok {
		panic("missing config: influx url")
	}
	if token, ok = os.LookupEnv("INFLUX_TOKEN"); !ok {
		panic("missing config: influx token")
	}
	if org, ok = os.LookupEnv("INFLUX_ORG"); !ok {
		panic("missing config: influx org")
	}
	if bucket, ok = os.LookupEnv("INFLUX_BUCKET"); !ok {
		panic("missing config: influx bucket")
	}
	client := influxdb2.NewClient(url, token)
	writer := client.WriteAPI(org, bucket)
	return writer, func() {
		writer.Flush()
		client.Close()
	}
}

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	flag.Parse()
	duration, err := time.ParseDuration(opts.duration)
	if err != nil {
		panic(err)
	}
	bucket, key := decodeBucketParts(opts.s3Path)
	if opts.generateObjects {
		generateTestObjects(bucket, key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(opts.concurrency)
	mw, close := createMetricWriter()
	defer close()
	mt := NewMetricRetryer(mw)
	s3ClientFactory := getS3Client(mt)
	for i := 0; i < opts.concurrency; i++ {
		c := &Client{
			id:            i,
			wg:            wg,
			provider:      s3ClientFactory,
			bucket:        bucket,
			key:           fmt.Sprintf("%s/%d/%d.obj", key, i, i),
			metricsWriter: mw,
		}
		c.Go(ctx)
	}
	wg.Wait()
}
