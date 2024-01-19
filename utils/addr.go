package utils

import (
	"net"
	"strings"
)

// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw, str string
}

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func (s *staticAddr) Network() string { return s.ntw }
func (s *staticAddr) String() string  { return s.str }

// AddrRepr a string representation of the server address implements net.Addr
func AddrRepr(server string) (net.Addr, error) {
	var nAddr net.Addr
	if strings.Contains(server, "/") {
		addr, _ := net.ResolveUnixAddr("unix", server)
		nAddr = newStaticAddr(addr)
	} else {
		tcpAddr, err := net.ResolveTCPAddr("tcp", server)
		if err != nil {
			return nil, err
		}
		nAddr = newStaticAddr(tcpAddr)
	}

	return nAddr, nil
}
