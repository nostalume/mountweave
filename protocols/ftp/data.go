package ftp

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

func (x *session) resetData() error {
	x.active = netip.AddrPort{}
	if x.passive == nil {
		return nil
	}
	l := x.passive
	x.passive = nil
	return l.Close()
}

func (x *session) passiveMode(cmd, arg string) error {
	if x.options.ListenPassive == nil {
		return x.reply(502, "Passive mode unavailable")
	}
	if cmd == "EPSV" && arg != "" {
		return x.reply(504, "EPSV argument unavailable")
	}
	if err := x.resetData(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(x.ctx, x.options.DataConnectTimeout)
	defer cancel()
	l, address, err := x.options.ListenPassive(ctx, x.conn.LocalAddr(), x.conn.RemoteAddr())
	if err != nil || l == nil || !address.IsValid() || address.Port() == 0 || cmd == "PASV" && !address.Addr().Is4() {
		if l != nil {
			if closeErr := l.Close(); closeErr != nil {
				return errors.Join(err, closeErr)
			}
		}
		return x.reply(425, "Passive endpoint unavailable")
	}
	x.passive = l
	if cmd == "EPSV" {
		return x.reply(229, fmt.Sprintf("Entering Extended Passive Mode (|||%d|)", address.Port()))
	}
	a := address.Addr().As4()
	return x.reply(227, fmt.Sprintf("Entering Passive Mode (%d,%d,%d,%d,%d,%d)", a[0], a[1], a[2], a[3], address.Port()/256, address.Port()%256))
}

func (x *session) activeMode(cmd, arg string) error {
	if x.options.DialActive == nil {
		return x.reply(502, "Active mode unavailable")
	}
	var target netip.AddrPort
	if cmd == "PORT" {
		parts := strings.Split(arg, ",")
		if len(parts) == 6 {
			var values [6]byte
			valid := true
			for i, p := range parts {
				n, err := strconv.ParseUint(p, 10, 8)
				if err != nil {
					valid = false
					break
				}
				values[i] = byte(n)
			}
			if valid {
				target = netip.AddrPortFrom(netip.AddrFrom4([4]byte{values[0], values[1], values[2], values[3]}), uint16(values[4])*256+uint16(values[5]))
			}
		}
	} else if len(arg) > 0 && arg[0] >= 33 && arg[0] <= 126 {
		parts := strings.Split(arg, string(arg[0]))
		if len(parts) == 5 && parts[0] == "" && parts[4] == "" {
			a, err := netip.ParseAddr(parts[2])
			port, pErr := strconv.ParseUint(parts[3], 10, 16)
			if err == nil && pErr == nil && (parts[1] == "1" && a.Is4() || parts[1] == "2" && a.Is6()) {
				target = netip.AddrPortFrom(a, uint16(port))
			}
		}
	}
	if !target.IsValid() || target.Port() == 0 {
		return x.reply(501, "Numeric data endpoint required")
	}
	if err := x.resetData(); err != nil {
		return err
	}
	x.active = target
	return x.reply(200, "Data endpoint accepted")
}
