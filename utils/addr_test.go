package utils

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStaticAddr(t *testing.T) {
	tcpAddr := &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 8080,
	}
	staticAddr := newStaticAddr(tcpAddr)
	if staticAddr.Network() != tcpAddr.Network() {
		t.Errorf("Expected Network() to be %s, got %s", tcpAddr.Network(), staticAddr.Network())
	}
	if staticAddr.String() != tcpAddr.String() {
		t.Errorf("Expected String() to be %s, got %s", tcpAddr.String(), staticAddr.String())
	}
}

func TestAddrRepr(t *testing.T) {
	type args struct {
		server string
	}
	tests := []struct {
		name    string
		args    args
		want    net.Addr
		wantErr bool
	}{
		{
			name:    "invalid address",
			args:    args{server: "invalid-address"},
			want:    nil,
			wantErr: true,
		},
		{
			name: "unix",
			args: args{server: "/var/unix.sock"},
			want: &staticAddr{
				ntw: "unix",
				str: "/var/unix.sock",
			},
			wantErr: false,
		},
		{
			name: "tcp",
			args: args{server: "127.0.0.1:8080"},
			want: &staticAddr{
				ntw: "tcp",
				str: "127.0.0.1:8080",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AddrRepr(tt.args.server)
			if tt.wantErr {
				assert.NotNilf(t, err, fmt.Sprintf("AddrRepr(%v) Expected an error, got nil", tt.args.server))
			}
			assert.Equalf(t, tt.want, got, "AddrRepr(%v)", tt.args.server)
		})
	}
}
