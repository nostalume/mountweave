package browser

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"
)

// StartHTTPSProxy starts a loopback CONNECT proxy for one browser launch.
// Configure that browser with proxy-server set to the returned address and
// proxy-bypass-list set to <-loopback>. The browser remains responsible for TLS.
// Closing the proxy ends its listener and active tunnels.
func StartHTTPSProxy(ctx context.Context) (string, func() error, error) {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	life, cancel := context.WithCancel(ctx)
	var gate sync.Mutex
	var requests sync.WaitGroup
	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    16 << 10,
		ErrorLog:          log.New(io.Discard, "", 0),
		BaseContext:       func(net.Listener) context.Context { return life },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gate.Lock()
			if life.Err() != nil {
				gate.Unlock()
				return
			}
			requests.Add(1)
			gate.Unlock()
			defer requests.Done()
			serveHTTPSConnect(w, r)
		}),
	}
	closeProxy := sync.OnceValue(func() error {
		cancel()
		err := server.Close()
		_ = listener.Close()
		gate.Lock()
		gate.Unlock()
		requests.Wait()
		return err
	})
	context.AfterFunc(life, func() { _ = closeProxy() })
	go func() { _ = server.Serve(listener) }()
	return "http://" + listener.Addr().String(), closeProxy, nil
}

func serveHTTPSConnect(w http.ResponseWriter, r *http.Request) {
	host, port, err := net.SplitHostPort(r.Host)
	if r.Method != http.MethodConnect || err != nil || host == "" || port != "443" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	dial, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	upstream, status := dialPublicHTTPS(dial, host)
	cancel()
	if upstream == nil {
		w.WriteHeader(status)
		return
	}
	defer upstream.Close()
	downstream, buffered, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	defer downstream.Close()
	stop := context.AfterFunc(r.Context(), func() {
		_ = upstream.Close()
		_ = downstream.Close()
	})
	defer stop()
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil || buffered.Flush() != nil {
		return
	}
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		_, _ = io.Copy(upstream, buffered.Reader)
		_ = upstream.CloseWrite()
	}()
	_, _ = io.Copy(downstream, upstream)
	_ = downstream.Close()
	_ = upstream.Close()
	<-sent
}

func dialPublicHTTPS(ctx context.Context, host string) (*net.TCPConn, int) {
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, http.StatusBadGateway
	}
	interfaces, err := net.InterfaceAddrs()
	if err != nil {
		return nil, http.StatusBadGateway
	}
	local := make(map[netip.Addr]bool, len(interfaces))
	for _, address := range interfaces {
		prefix, err := netip.ParsePrefix(address.String())
		if err != nil {
			return nil, http.StatusBadGateway
		}
		local[prefix.Addr().Unmap()] = true
	}
	status := http.StatusForbidden
	var dialer net.Dialer
	for _, address := range addresses {
		address = address.Unmap()
		if local[address] || !publicHTTPSIP(address) {
			continue
		}
		status = http.StatusBadGateway
		// Dial the validated address directly, without resolving the name again.
		target := netip.AddrPortFrom(address, 443).String()
		conn, err := dialer.DialContext(ctx, "tcp", target)
		if err == nil {
			return conn.(*net.TCPConn), 0
		}
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			break
		}
	}
	return nil, status
}

var internetIPv6 = netip.MustParsePrefix("2000::/3")

var specialUseAddresses = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

func publicHTTPSIP(address netip.Addr) bool {
	address = address.Unmap()
	if address.Zone() != "" || !address.IsGlobalUnicast() || address.IsPrivate() ||
		address.Is6() && !internetIPv6.Contains(address) {
		return false
	}
	for _, blocked := range specialUseAddresses {
		if blocked.Contains(address) {
			return false
		}
	}
	return true
}
