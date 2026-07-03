package p2p

import (
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

func TestShouldAdvertiseLANAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		{name: "lan ipv4", addr: "/ip4/192.168.1.66/tcp/45311", want: true},
		{name: "ten dot lan ipv4", addr: "/ip4/10.1.2.3/tcp/45311", want: true},
		{name: "ula ipv6", addr: "/ip6/fd00::1/tcp/45311", want: true},
		{name: "loopback", addr: "/ip4/127.0.0.1/tcp/45311", want: false},
		{name: "unspecified", addr: "/ip4/0.0.0.0/tcp/45311", want: false},
		{name: "link local ipv4", addr: "/ip4/169.254.1.2/tcp/45311", want: false},
		{name: "link local ipv6", addr: "/ip6/fe80::1/tcp/45311", want: false},
		{name: "docker bridge", addr: "/ip4/172.17.0.1/tcp/45311", want: false},
		{name: "docker compose bridge", addr: "/ip4/172.19.0.1/tcp/45311", want: false},
		{name: "higher 172 private lan", addr: "/ip4/172.20.0.5/tcp/45311", want: true},
		{name: "wsl virtual adapter", addr: "/ip4/10.255.255.254/tcp/45311", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := ma.NewMultiaddr(tt.addr)
			if err != nil {
				t.Fatal(err)
			}
			if got := shouldAdvertiseLANAddr(addr); got != tt.want {
				t.Fatalf("shouldAdvertiseLANAddr(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}

func TestFilterAdvertisedAddrs(t *testing.T) {
	addrs := []ma.Multiaddr{
		ma.StringCast("/ip4/127.0.0.1/tcp/45311"),
		ma.StringCast("/ip4/192.168.1.66/tcp/45311"),
		ma.StringCast("/ip4/172.18.0.1/tcp/45311"),
	}

	got := filterAdvertisedAddrs(addrs)
	if len(got) != 1 {
		t.Fatalf("filtered addrs length = %d, want 1: %v", len(got), got)
	}
	if got[0].String() != "/ip4/192.168.1.66/tcp/45311" {
		t.Fatalf("filtered addr = %q, want LAN addr", got[0])
	}
}
