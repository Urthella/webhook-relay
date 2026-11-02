package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"syscall"
)

// ErrBlockedAddress means an endpoint resolved to an address the relay refuses to reach.
var ErrBlockedAddress = errors.New("blocked address")

var blockedRanges = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),   // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),    // documentation
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // documentation
	netip.MustParsePrefix("203.0.113.0/24"),  // documentation
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64, which can reach IPv4 addresses behind it
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
}

// isPublic says whether an address is on the public internet. Anyone who can register an endpoint can
// make this server send requests, so without this check they could reach its internal network or a
// cloud metadata service.
func isPublic(ip netip.Addr) bool {
	ip = ip.Unmap() // ::ffff:127.0.0.1 is 127.0.0.1
	// IsGlobalUnicast already rules out loopback, link-local (so cloud metadata), multicast and unspecified.
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	for _, r := range blockedRanges {
		if r.Contains(ip) {
			return false
		}
	}
	return true
}

// guardedDialer checks the address it's about to connect to, after DNS has resolved. Checking the URL
// when it's registered isn't enough: a hostname can resolve to a public address then and a private one
// later, which is DNS rebinding.
func guardedDialer(allowPrivate bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{}
	if !allowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("%w: %s", ErrBlockedAddress, address)
			}
			ip, err := netip.ParseAddr(host)
			if err != nil || !isPublic(ip) {
				return fmt.Errorf("%w: %s", ErrBlockedAddress, host)
			}
			return nil
		}
	}
	return dialer.DialContext
}

// checkEndpointURL refuses URLs that could never be delivered to safely.
func checkEndpointURL(raw string, allowPrivate bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("url must be absolute")
	}
	if u.User != nil {
		return errors.New("url must not contain credentials")
	}
	if u.Scheme != "https" && (!allowPrivate || u.Scheme != "http") {
		return errors.New("url must use https")
	}
	if allowPrivate {
		return nil
	}
	// Fail early on an obvious literal address. The dialer is what actually enforces this.
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !isPublic(ip) {
		return fmt.Errorf("%w: %s", ErrBlockedAddress, ip)
	}
	return nil
}
