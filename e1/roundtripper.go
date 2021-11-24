package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	metrics "github.com/influxdata/influxdb-client-go/v2/api"
)

type TracingRoundTripper struct {
	inner http.RoundTripper
	mw    metrics.WriteAPI
}

func (r *TracingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var (
		host      string
		dnsStart  time.Time
		getConn   time.Time
		connStart time.Time
		tlsStart  time.Time
	)
	trace := httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = time.Now()
			host = info.Host
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			r.mw.WritePoint(influxdb2.NewPointWithMeasurement("dns-lookup").AddTag("host", host).AddField("duration", time.Since(dnsStart).Microseconds()))
		},
		GetConn: func(hostPort string) {
			getConn = time.Now()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			r.mw.WritePoint(influxdb2.NewPointWithMeasurement("wait-for-conn").AddField("duration", time.Since(getConn).Microseconds()))
			if info.Reused {
				r.mw.WritePoint(influxdb2.NewPointWithMeasurement("conn-reused").AddField("count", 1))
			} else {
				r.mw.WritePoint(influxdb2.NewPointWithMeasurement("conn-created").AddField("count", 1))
			}
		},
		PutIdleConn: func(err error) {
			r.mw.WritePoint(influxdb2.NewPointWithMeasurement("put-idle-conn").AddField("success", err == nil))
		},
		ConnectStart: func(network, addr string) {
			connStart = time.Now()
		},
		ConnectDone: func(network, addr string, err error) {
			r.mw.WritePoint(influxdb2.NewPointWithMeasurement("conn-handshake").AddField("duration", time.Since(connStart).Microseconds()))
		},
		TLSHandshakeStart: func() {
			tlsStart = time.Now()
		},
		TLSHandshakeDone: func(s tls.ConnectionState, err error) {
			r.mw.WritePoint(influxdb2.NewPointWithMeasurement("tls-handshake").AddField("duration", time.Since(tlsStart).Microseconds()))
		},
	}
	wreq := req.WithContext(httptrace.WithClientTrace(req.Context(), &trace))
	return r.inner.RoundTrip(wreq)
}

func NewTracingRoundTripper(inner http.RoundTripper, mw metrics.WriteAPI) http.RoundTripper {
	return &TracingRoundTripper{
		inner: inner,
		mw:    mw,
	}
}
