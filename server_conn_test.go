//go:build !js

package mosh

import (
	"encoding/base64"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type memoryServerConn struct {
	incoming  chan []byte
	outgoing  chan []byte
	done      chan struct{}
	peerDone  chan struct{}
	closeOnce sync.Once
	deadline  time.Time
}

func memoryServerPair() (*memoryServerConn, *memoryServerConn) {
	toServer, toClient := make(chan []byte, 100), make(chan []byte, 100)
	serverDone, clientDone := make(chan struct{}), make(chan struct{})
	return &memoryServerConn{incoming: toServer, outgoing: toClient, done: serverDone, peerDone: clientDone},
		&memoryServerConn{incoming: toClient, outgoing: toServer, done: clientDone, peerDone: serverDone}
}

func (conn *memoryServerConn) Read(buffer []byte) (int, error) {
	var timer *time.Timer
	var timeout <-chan time.Time
	if !conn.deadline.IsZero() {
		timer = time.NewTimer(time.Until(conn.deadline))
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-conn.done:
		return 0, net.ErrClosed
	case <-conn.peerDone:
		return 0, net.ErrClosed
	case <-timeout:
		return 0, os.ErrDeadlineExceeded
	case datagram := <-conn.incoming:
		return copy(buffer, datagram), nil
	}
}

func (conn *memoryServerConn) ReadFromUDP(buffer []byte) (int, *net.UDPAddr, error) {
	count, err := conn.Read(buffer)
	return count, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 60001}, err
}

func (conn *memoryServerConn) Write(buffer []byte) (int, error) {
	select {
	case <-conn.done:
		return 0, net.ErrClosed
	case <-conn.peerDone:
		return 0, net.ErrClosed
	case conn.outgoing <- append([]byte(nil), buffer...):
		return len(buffer), nil
	}
}

func (conn *memoryServerConn) WriteToUDP(buffer []byte, _ *net.UDPAddr) (int, error) {
	return conn.Write(buffer)
}

func (conn *memoryServerConn) SetReadDeadline(deadline time.Time) error {
	conn.deadline = deadline
	return nil
}

func (conn *memoryServerConn) Close() error {
	conn.closeOnce.Do(func() { close(conn.done) })
	return nil
}

func TestNewServerConnRejectsNil(t *testing.T) {
	if _, err := NewServerConn("", nil, 60001); err == nil {
		t.Fatal("nil datagram connection was accepted")
	}
}

func TestNewServerConnOwnsTransport(t *testing.T) {
	serverConn, peerConn := memoryServerPair()
	defer peerConn.Close()
	server, err := NewServerConn("/bin/sh", serverConn, 60001)
	if err != nil {
		t.Fatal(err)
	}
	if server.conn != serverConn || server.Port() != 60001 {
		t.Fatal("constructor did not retain caller's transport and virtual port")
	}
	key, err := base64.RawStdEncoding.DecodeString(server.KeyBase64())
	if err != nil || len(key) != 16 {
		t.Fatalf("key length=%d error=%v", len(key), err)
	}
	server.Close()
	server.Close()
	select {
	case <-serverConn.done:
	default:
		t.Fatal("server close did not close caller's transport")
	}
}

func TestServeRWOverMemoryDatagrams(t *testing.T) {
	serverConn, clientConn := memoryServerPair()
	defer serverConn.Close()
	defer clientConn.Close()
	server, err := NewServerConn("", serverConn, 60001)
	if err != nil {
		t.Fatal(err)
	}
	terminal, application := net.Pipe()
	defer terminal.Close()
	defer application.Close()
	resizes := make(chan [2]uint16, 1)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.ServeRW(terminal, func(cols, rows uint16) {
			resizes <- [2]uint16{cols, rows}
		})
	}()
	key, err := base64.RawStdEncoding.DecodeString(server.KeyBase64())
	if err != nil {
		t.Fatal(err)
	}
	ocb, err := NewOCB(key)
	if err != nil {
		t.Fatal(err)
	}
	client, err := DialConn(clientConn, ocb)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Resize(91, 31)
	select {
	case size := <-resizes:
		if size != [2]uint16{91, 31} {
			t.Fatalf("resize=%v", size)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resize did not traverse memory datagrams")
	}
	input := make(chan string, 1)
	go func() {
		buffer := make([]byte, 5)
		_, err := io.ReadFull(application, buffer)
		if err != nil {
			input <- err.Error()
			return
		}
		input <- string(buffer)
	}()
	client.Send([]byte("hello"))
	select {
	case text := <-input:
		if text != "hello" {
			t.Fatalf("input=%q", text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("input did not traverse memory datagrams")
	}
	if _, err := application.Write([]byte("VIRTUAL_SERVER_READY\r\n")); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(output.String(), "VIRTUAL_SERVER_READY") {
		if time.Now().After(deadline) {
			t.Fatalf("missing terminal output: %q", output.String())
		}
		output.Write(client.Recv(100 * time.Millisecond))
	}
	application.Close()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("memory server did not shut down on terminal EOF")
	}
	select {
	case <-serverConn.done:
	default:
		t.Fatal("ServeRW did not close memory transport")
	}
}
