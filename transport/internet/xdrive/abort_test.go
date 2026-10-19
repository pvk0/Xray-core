package xdrive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func serveAndDial(t *testing.T, streamSettings *internet.MemoryStreamConfig) (client, server *Conn, listener *Listener) {
	t.Helper()

	accepted := make(chan stat.Connection, 1)
	l, err := Serve(context.Background(), net.LocalHostIP, net.Port(0), streamSettings, func(conn stat.Connection) {
		accepted <- conn
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	c, err := Dial(context.Background(), net.Destination{}, streamSettings)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	select {
	case s := <-accepted:
		return c.(*Conn), s.(*Conn), l.(*Listener)
	case <-time.After(testPatience):
		t.Fatal("listener did not accept the session")
	}
	return nil, nil, nil
}

// expectBroken writes like a mux client keeps doing and wants Read to fail
// instead of waiting forever for a peer that is gone
func expectBroken(t *testing.T, conn *Conn, within time.Duration) {
	t.Helper()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				if _, err := conn.Write([]byte("keepalive")); err != nil {
					return
				}
			}
		}
	}()

	conn.SetReadDeadline(time.Now().Add(within))
	buf := make([]byte, 64)
	for {
		_, err := conn.Read(buf)
		if err == nil {
			continue
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the session still looks alive after %v although its peer is gone", within)
		}
		return
	}
}

func TestListenerCloseAbortsSessions(t *testing.T) {
	client, server, listener := serveAndDial(t, settings(t.TempDir()))

	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	expectRead(t, server, "ping")

	// what an API reload of the inbound does, as 3x-ui applies config changes
	listener.Close()

	expectBroken(t, client, 10*time.Second)
}

func TestPeerStopsReading(t *testing.T) {
	ss := settings(t.TempDir())
	ss.ProtocolSettings.(*Config).HoleTimeoutMs = 200
	client, server, _ := serveAndDial(t, ss)

	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	expectRead(t, server, "ping")

	// the peer dies without a word, as a killed process does
	server.cancel()

	expectBroken(t, client, 10*time.Second)
}

type countingStorage struct {
	Storage
	lists atomic.Int64
}

func (s *countingStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	s.lists.Add(1)
	return s.Storage.List(ctx, prefix)
}

func (s *countingStorage) probesByName() bool {
	return true
}

func TestProbeSkipsListing(t *testing.T) {
	local, err := newLocalStorage(t.TempDir())
	if err != nil {
		t.Fatalf("newLocalStorage: %v", err)
	}
	storage := &countingStorage{Storage: local}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := paramsFromConfig(&Config{PollIntervalMs: 5, MaxPollIntervalMs: 200, EagerWindowMs: 10000})
	reader := newWALReader(ctx, storage, "probe", p)

	const segments = 20
	for i := 0; i < segments; i++ {
		time.Sleep(20 * time.Millisecond)
		want := fmt.Sprintf("segment %d", i)
		if err := storage.Put(ctx, objectName("probe", int64(i), segSuffix), []byte(want)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		select {
		case data := <-reader.ch:
			if string(data) != want {
				t.Fatalf("read %q, want %q", data, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("segment %d did not arrive", i)
		}
	}
	if lists := storage.lists.Load(); lists >= segments/2 {
		t.Fatalf("listed %d times for %d segments, want the reader to fetch them by name", lists, segments)
	}

	if err := storage.Put(ctx, objectName("probe", segments, endSuffix), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	select {
	case _, ok := <-reader.ch:
		if ok {
			t.Fatal("read data past the end")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the reader missed the end while probing")
	}
}
