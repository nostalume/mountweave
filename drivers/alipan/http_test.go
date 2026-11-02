package alipan_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/nostalume/mountweave"
	driver "github.com/nostalume/mountweave/drivers/alipan"
	"github.com/nostalume/mountweave/drivers/alipan/credential"
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

// This case adds real HTTP cancellation to the existing protocol and save-fault
// fixtures. Its credentials and save acknowledgement are synthetic.
func TestClosedHTTPRefreshAndRetirement(t *testing.T) {
	var exchanges, saves atomic.Int32
	canceled := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/access_token":
			exchanges.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"token_type": "Bearer", "access_token": "synthetic-access", "refresh_token": "synthetic-new-refresh", "expires_in": 3600})
		case "/adrive/v1.0/openFile/list":
			if saves.Load() != 1 {
				t.Error("access used before save acknowledgement")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{file("file-id", "song")}})
		case "/adrive/v1.0/openFile/getDownloadUrl":
			_ = json.NewEncoder(w).Encode(map[string]string{"url": "https://" + r.Host + "/download", "method": "GET"})
		case "/download":
			if r.Header.Get("Authorization") != "" {
				t.Error("provider credential forwarded to content request")
			}
			w.Header().Set("Content-Length", "10")
			_, _ = io.WriteString(w, "abc")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(canceled)
		default:
			t.Error("unexpected native route")
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := closedClient(t, server)
	if _, err := client.Get("https://outside.invalid/"); err == nil {
		t.Fatal("external destination admitted")
	}
	oauth := driver.OAuth{Client: client, Endpoint: server.URL, ClientID: "synthetic-app", ClientSecret: "synthetic-secret"}
	source, err := credential.New(credential.Record{Token: credential.Token{Refresh: "synthetic-old-refresh"}}, oauth.Refresh, func(_ context.Context, record credential.Record) error {
		if record.Pending {
			return nil
		}
		token := record.Token

		if token.Refresh != "synthetic-new-refresh" {
			t.Error("rotated token lost")
		}
		saves.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	storage, err := driver.New(driver.Options{Client: client, Endpoint: server.URL, DriveID: "drive", RootID: "root", Credentials: source})
	if err != nil {
		_ = source.Close()
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
	// This watchdog is a test bound, not a deployment policy.
	watchdog, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	defer func() {
		if err := reader.Close(watchdog); err != nil {
			t.Error(err)
		}
	}()
	opened, err := reader.Open(watchdog, "reader", "/cloud/song", nil)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 3)
	if _, err = io.ReadFull(opened.Body, data); err != nil {
		t.Fatal(err)
	}
	if err = mount.Retire(watchdog); err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadAll(opened.Body); !errors.Is(err, context.Canceled) {
		t.Fatalf("retired HTTP body: %v", err)
	}
	select {
	case <-canceled:
	case <-watchdog.Done():
		t.Fatal("server did not observe retirement")
	}
	if exchanges.Load() != 1 || saves.Load() != 1 {
		t.Fatal("duplicate refresh or save")
	}
	if _, err = source.Access(watchdog); !errors.Is(err, credential.ErrClosed) {
		t.Fatal("credential owner survived retirement", err)
	}
}
