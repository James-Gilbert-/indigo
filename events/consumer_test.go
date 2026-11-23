package events_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleRepoStreamIdleTimeout(t *testing.T) {
	idle := []events.RepoStreamOption{events.WithRepoStreamIdleTimeout(2 * time.Minute)}
	tests := []struct {
		name      string
		opts      []events.RepoStreamOption
		events    int           // sent one per minute, then only pongs
		timeoutAt time.Duration // 0: never times out
	}{
		{"silent stream times out despite pongs", idle, 0, 2 * time.Minute},
		{"each event resets the deadline", idle, 5, 7 * time.Minute},
		{"without the option a silent stream stays open", nil, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				done := handleStream(t, fakeRelay(t, tt.events), tt.opts...)
				if tt.timeoutAt == 0 {
					advance(time.Hour)
					require.Empty(t, done, "stream exited without an idle timeout")
					return
				}
				advance(tt.timeoutAt - time.Second)
				require.Empty(t, done, "stream exited before the deadline")
				advance(2 * time.Second)
				require.Len(t, done, 1, "stream still running after the deadline")
				err := <-done
				require.ErrorIs(t, err, events.ErrIdleTimeout)
				var netErr net.Error
				require.True(t, errors.As(err, &netErr) && netErr.Timeout(), "want wrapped net timeout, got %v", err)
			})
		})
	}
}

func TestHandleRepoStreamPongTimeoutNotDelayed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			// Send one event at t=10s, then go completely silent (no pongs or events).
			time.Sleep(10 * time.Second)
			if conn.WriteMessage(websocket.BinaryMessage, identityFrame(t, 1)) != nil {
				return
			}
			<-t.Context().Done()
		}))
		dialer := websocket.Dialer{NetDialContext: srv.Client().Transport.(*http.Transport).DialContext}
		conn, _, err := dialer.Dial("ws://example.com/", nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		done := handleStream(t, conn, events.WithRepoStreamIdleTimeout(5*time.Minute))

		// Pong deadline is 1m after the last event (t=1m10s), well before the 5m idle timeout.
		advance(time.Minute + 11*time.Second)
		require.Len(t, done, 1, "dead connection should time out at 1m pong deadline, not 5m idle timeout")
		err = <-done
		require.NotErrorIs(t, err, events.ErrIdleTimeout)
		var netErr net.Error
		require.True(t, errors.As(err, &netErr) && netErr.Timeout(), "want net timeout, got %v", err)
	})
}

func TestHandleRepoStreamSlowSchedulerNotIdleTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unblock := make(chan struct{})
		inWork := make(chan struct{}, 1)
		sched := sequential.NewScheduler("test", func(ctx context.Context, _ *events.XRPCStreamEvent) error {
			select {
			case inWork <- struct{}{}:
			default:
			}
			select {
			case <-unblock:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		conn := fakeRelay(t, 2)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		finished := make(chan struct{})
		go func() {
			done <- events.HandleRepoStream(ctx, conn, sched, nil, events.WithRepoStreamIdleTimeout(2*time.Minute))
			close(finished)
		}()
		t.Cleanup(func() {
			cancel()
			<-finished
			advance(time.Minute)
		})

		// Wait for event 1 (sent at t=1m) to enter AddWork, then block for 3m (> 2m idleTimeout).
		advance(time.Minute)
		<-inWork
		advance(3 * time.Minute)
		require.Empty(t, done, "slow downstream AddWork must not trigger ErrIdleTimeout")

		close(unblock)
		advance(time.Second)
		require.Empty(t, done, "stream should stay healthy after AddWork unblocks")
	})
}

// fakeRelay answers pings continuously (via gorilla's default ping handler),
// sends n identity events one per minute, and then sends no further data.
func fakeRelay(t *testing.T, n int) *websocket.Conn {
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		go func() {
			for seq := 1; seq <= n; seq++ {
				select {
				case <-t.Context().Done():
					return
				case <-time.After(time.Minute):
				}
				if conn.WriteMessage(websocket.BinaryMessage, identityFrame(t, seq)) != nil {
					return
				}
			}
		}()
		for _, _, err := conn.ReadMessage(); err == nil; _, _, err = conn.ReadMessage() {
		}
	}))
	dialer := websocket.Dialer{NetDialContext: srv.Client().Transport.(*http.Transport).DialContext}
	conn, _, err := dialer.Dial("ws://example.com/", nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func identityFrame(t *testing.T, seq int) []byte {
	var buf bytes.Buffer
	evt := &events.XRPCStreamEvent{RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
		Seq: int64(seq), Did: "did:example:alice", Time: "2026-01-01T00:00:00Z",
	}}
	assert.NoError(t, evt.Serialize(&buf)) // called from the server goroutine: assert, not require
	return buf.Bytes()
}

// handleStream runs HandleRepoStream in the background and stops it at cleanup.
func handleStream(t *testing.T, conn *websocket.Conn, opts ...events.RepoStreamOption) <-chan error {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	sched := sequential.NewScheduler("test", func(context.Context, *events.XRPCStreamEvent) error { return nil })
	go func() {
		done <- events.HandleRepoStream(ctx, conn, sched, nil, opts...)
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
		advance(time.Minute) // let the fake relay notice the closed connection
	})
	return done
}

// advance moves the fake clock forward and waits for the bubble to settle.
func advance(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}
