package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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
	metricsBuffer       int
}

type tokenBucket struct {
	mutex        *sync.Mutex
	maxPerSecond int
	tokens       int
	lastActivity time.Time
}

func (b *tokenBucket) Consume() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	interval := time.Since(b.lastActivity)
	if interval > time.Second {
		b.tokens = b.maxPerSecond
		b.lastActivity = time.Now()
	}
	if b.tokens == 0 {
		return false
	}
	b.tokens -= 1
	return true
}

type event struct {
	timestamp int64
	duration  time.Duration
}

type telem struct {
	cw     *cloudwatch.Client
	in     chan event
	buffer []event
	tokens *tokenBucket
	done   chan struct{}
}

func (t *telem) Start() {
	fmt.Println("telem process started")
	for {
		select {
		case d, ok := <-t.in:
			if !ok {
				fmt.Println("input channel closed")
				t.sendBatches(true)
				close(t.done)
				return
			}
			t.buffer = append(t.buffer, d)
			t.sendBatches(false)
		case <-time.After(time.Millisecond * 200):
			t.sendBatches(true)
		}
	}
}

func (t *telem) Record(d time.Duration) {
	t.in <- event{
		timestamp: time.Now().Unix(),
		duration:  d,
	}
}

func (t *telem) Done() <-chan struct{} {
	return t.done
}

func (t *telem) sendBatches(flush bool) {
	const maxBatchSize int = 40
	for {
		if len(t.buffer) == 0 {
			return
		}
		if len(t.buffer) < maxBatchSize && !flush {
			return
		}
		if !t.tokens.Consume() {
			return
		}
		batchSize := maxBatchSize
		if len(t.buffer) < maxBatchSize {
			batchSize = len(t.buffer)
		}
		batch := t.buffer[:batchSize]
		metrics := make([]types.MetricDatum, batchSize)
		for i, e := range batch {
			metrics[i] = types.MetricDatum{
				MetricName:        aws.String("get-object-duration"),
				Unit:              types.StandardUnitMilliseconds,
				Value:             aws.Float64(float64(e.duration.Milliseconds())),
				Timestamp:         aws.Time(time.Unix(e.timestamp, 0)),
				StorageResolution: aws.Int32(1),
			}
		}
		_, err := t.cw.PutMetricData(context.TODO(), &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String("gohttp"),
			MetricData: metrics,
		})
		if err != nil {
			log.Printf("INFO: error publishing metric %v", err)
		} else {
			t.buffer = t.buffer[batchSize:]
		}
	}
}

type client struct {
	s3Bucket string
	s3Key    string
	s3       func() *s3.Client
	wg       *sync.WaitGroup
	telem    *telem
	count    int
}

func (c *client) Go(ctx context.Context) {
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				start := time.Now()
				res, err := c.s3().GetObject(ctx, &s3.GetObjectInput{
					Bucket: &c.s3Bucket,
					Key:    &c.s3Key,
				})
				if err != nil {
					log.Printf("INFO: GetObject failed %v", err)
					if serr, ok := err.(smithy.APIError); ok {
						log.Printf("INFO: GetObject faild %v", serr.ErrorMessage())
						if serr.ErrorFault() != smithy.FaultClient {
							return
						}
					}
					continue
				}
				_, err = ioutil.ReadAll(res.Body)
				if err != nil {
					log.Printf("INFO: reading object data failed %v", err)
				}
				res.Body.Close()
				c.count++
				duration := time.Since(start)
				c.telem.Record(duration)
			}
		}
	}()
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
	flag.IntVar(&opts.metricsBuffer, "metrics-buffer", 20000000, "number of metrics to buffer")
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
	tb := &tokenBucket{
		maxPerSecond: 150,
		tokens:       150,
		lastActivity: time.Now(),
		mutex:        &sync.Mutex{},
	}
	tchan := make(chan event, opts.metricsBuffer)
	tel := &telem{
		cw:     getCloudwatchClient(),
		in:     tchan,
		buffer: make([]event, 0),
		done:   make(chan struct{}),
		tokens: tb,
	}
	go tel.Start()
	mt := newMetricRetyer()
	clients := make([]*client, opts.concurrency)
	s3ClientFactory := getS3Client(mt)
	for i := 0; i < opts.concurrency; i++ {
		c := &client{
			wg:       wg,
			s3:       s3ClientFactory,
			s3Bucket: bucket,
			s3Key:    fmt.Sprintf("%s/%d/%d.obj", key, i, i),
			telem:    tel,
		}
		clients[i] = c
		go c.Go(ctx)
	}
	wg.Wait()
	sum := 0
	for _, c := range clients {
		sum += c.count
	}
	fmt.Printf("total requests: %d\n", sum)
	fmt.Printf("%f req/sec\n", float64(sum)/duration.Seconds())
	fmt.Printf("total retries: %d\n", mt.retryCount)
	close(tchan)
	<-tel.Done()
}

type noopRetryer struct {
}

func (r *noopRetryer) IsErrorRetryable(error) bool {
	return false
}

func (r *noopRetryer) MaxAttempts() int {
	return 0
}

func (r *noopRetryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	return time.Duration(0), nil
}

func (r *noopRetryer) GetRetryToken(ctx context.Context, opErr error) (func(error) error, error) {
	return func(err error) error {
		return nil
	}, nil
}

func (r *noopRetryer) GetInitialToken() func(error) error {
	return func(err error) error {
		return nil
	}
}

func newNoopRetryer() aws.Retryer {
	return &noopRetryer{}
}

type metricRetryer struct {
	inner      aws.Retryer
	mu         *sync.Mutex
	retryCount int
}

func (r *metricRetryer) IsErrorRetryable(err error) bool {
	return r.inner.IsErrorRetryable(err)
}

func (r *metricRetryer) MaxAttempts() int {
	return r.inner.MaxAttempts()
}

func (r *metricRetryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	ir, err := r.inner.RetryDelay(attempt, opErr)
	if err == nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.retryCount++
	}
	return ir, err
}

func (r *metricRetryer) GetRetryToken(ctx context.Context, opErr error) (func(error) error, error) {
	return r.inner.GetRetryToken(ctx, opErr)
}

func (r *metricRetryer) GetInitialToken() func(error) error {
	return r.inner.GetInitialToken()
}

func newMetricRetyer() *metricRetryer {
	return &metricRetryer{
		inner: retry.NewStandard(),
		mu:    &sync.Mutex{},
	}
}
