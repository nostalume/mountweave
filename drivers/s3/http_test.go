package s3_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/s3"
)

func closedClient(t *testing.T, server *httptest.Server) *http.Client {
	t.Helper()
	origin, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialTLSContext = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != origin.Host {
			return nil, errors.New("destination outside closed test")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// The existing transport fixtures cover the protocol matrix. This case covers
// actual HTTP body cancellation and retirement on a confined TLS connection.
func TestClosedHTTPBodyCancellation(t *testing.T) {
	canceled := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			_, _ = io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
			return
		}
		w.Header().Set("Content-Length", "10")
		_, _ = io.WriteString(w, "abc")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	client := closedClient(t, server)
	if _, err := client.Get("https://outside.invalid/"); err == nil {
		t.Fatal("external destination admitted")
	}
	native := sdk.New(sdk.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, RetryMaxAttempts: 1, HTTPClient: client, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "synthetic-key", SecretAccessKey: "synthetic-secret"}, nil
	})})
	storage, err := driver.New(native, "bucket", "vault/")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := core.New(core.Options{Authorize: func(context.Context, core.Principal, core.Action, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := reader.Install("/cloud", storage)
	if err != nil {
		_ = storage.Close()
		t.Fatal(err)
	}
	// Watchdogs terminate broken tests; they are not product latency requirements.
	watchdog, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	defer func() {
		if err := reader.Close(watchdog); err != nil {
			t.Error(err)
		}
	}()
	active, cancel := context.WithCancel(watchdog)
	defer cancel()
	opened, err := reader.Open(active, "reader", "/cloud/a", nil)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 3)
	if _, err = io.ReadFull(opened.Body, data); err != nil || string(data) != "abc" {
		t.Fatal("prefix read failed", err)
	}
	cancel()
	if _, err = io.Copy(io.Discard, opened.Body); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled HTTP body: %v", err)
	}
	select {
	case <-canceled:
	case <-watchdog.Done():
		t.Fatal("server did not observe cancellation")
	}
	if err = mount.Retire(watchdog); err != nil {
		t.Fatal(err)
	}
}
