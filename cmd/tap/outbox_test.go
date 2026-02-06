package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

func TestBackoff(t *testing.T) {
	tests := []struct {
		retries int
		max     int
		wantMin time.Duration
		wantMax time.Duration
	}{
		{0, 10, 1 * time.Second, 2 * time.Second},
		{1, 10, 2 * time.Second, 3 * time.Second},
		{2, 10, 4 * time.Second, 5 * time.Second},
		{5, 10, 10 * time.Second, 11 * time.Second},  // capped
		{10, 10, 10 * time.Second, 11 * time.Second}, // capped
	}
	for _, tt := range tests {
		got := backoff(tt.retries, tt.max)
		if got < tt.wantMin || got >= tt.wantMax {
			t.Errorf("backoff(%d, %d) = %v, want [%v, %v)",
				tt.retries, tt.max, got, tt.wantMin, tt.wantMax)
		}
	}
}

func TestMatchesCollection(t *testing.T) {
	tests := []struct {
		collection string
		filters    []string
		want       bool
	}{
		{"app.bsky.feed.post", nil, true},
		{"app.bsky.feed.post", []string{"app.bsky.feed.post"}, true},
		{"app.bsky.feed.post", []string{"app.bsky.feed.*"}, true},
		{"app.bsky.feed.post", []string{"app.bsky.*"}, true},
		{"app.bsky.feed.post", []string{"app.bsky.graph.follow"}, false},
		{"app.bsky.feed.post", []string{"com.example.*"}, false},
		{"app.bsky.feed.post", []string{"no.match", "app.bsky.*"}, true},
	}
	for _, tt := range tests {
		if got := matchesCollection(tt.collection, tt.filters); got != tt.want {
			t.Errorf("matchesCollection(%q, %v) = %v, want %v",
				tt.collection, tt.filters, got, tt.want)
		}
	}
}

func TestParseOutboxMode(t *testing.T) {
	tests := []struct {
		webhookURL  string
		disableAcks bool
		want        OutboxMode
	}{
		{"http://example.localhost", false, OutboxModeWebhook},
		{"http://example.localhost", true, OutboxModeWebhook},
		{"", true, OutboxModeFireAndForget},
		{"", false, OutboxModeWebsocketAck},
	}
	for _, tt := range tests {
		if got := parseOutboxMode(tt.webhookURL, tt.disableAcks); got != tt.want {
			t.Errorf("parseOutboxMode(%q, %v) = %v, want %v",
				tt.webhookURL, tt.disableAcks, got, tt.want)
		}
	}
}

func TestWebhookClientSend_SuccessStatuses(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		password string
		wantAck  bool
	}{
		{"200 OK", 200, "", true},
		{"201 Created", 201, "", true},
		{"204 No Content", 204, "", true},
		{"200 with auth", 200, "secret", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotAuth string
			client := &WebhookClient{
				logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
				webhookURL:    "http://test.localhost/webhook",
				adminPassword: tt.password,
				httpClient: &http.Client{
					Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
						if u, p, ok := req.BasicAuth(); ok {
							gotAuth = u + ":" + p
						}
						return &http.Response{StatusCode: tt.status, Body: http.NoBody}, nil
					}),
				},
			}

			var ackCalled atomic.Bool
			client.Send(context.Background(), &OutboxEvt{ID: 1}, func(uint) { ackCalled.Store(true) })

			if got := ackCalled.Load(); got != tt.wantAck {
				t.Errorf("ackFn called = %v, want %v", got, tt.wantAck)
			}

			if tt.password != "" {
				wantAuth := "admin:" + tt.password
				if gotAuth != wantAuth {
					t.Errorf("basic auth = %q, want %q", gotAuth, wantAuth)
				}
			}
		})
	}
}

func TestWebhookClientPost_RequestFormat(t *testing.T) {
	var gotReq *http.Request
	var gotBody string
	client := &WebhookClient{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		webhookURL: "http://test.localhost/webhook",
		httpClient: &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				gotReq = req
				body, _ := io.ReadAll(req.Body)
				gotBody = string(body)
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			}),
		},
	}

	evt := &OutboxEvt{ID: 42, Event: []byte(`{"test":"data"}`)}
	err := client.Post(context.Background(), evt)
	if err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	if gotReq.Method != "POST" {
		t.Errorf("method = %q, want POST", gotReq.Method)
	}
	if ct := gotReq.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if ua := gotReq.Header.Get("User-Agent"); !strings.HasPrefix(ua, "tap/") {
		t.Errorf("User-Agent = %q, want tap/*", ua)
	}
	if gotBody != `{"test":"data"}` {
		t.Errorf("body = %q, want %q", gotBody, `{"test":"data"}`)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// Tests ctx.Done path in httpClient.Do
func TestWebhookClientSend_CancelDuringRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	client := newTestWebhookClient(func(req *http.Request) (*http.Response, error) {
		close(requestStarted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ackCalled, done := runSend(client, ctx)
	<-requestStarted
	cancel()

	assertSendReturns(t, done)
	assertNoAck(t, ackCalled)
}

// Tests ctx.Done path in backoff select
func TestWebhookClientSend_CancelDuringBackoff(t *testing.T) {
	requestDone := make(chan struct{})
	client := newTestWebhookClient(func(req *http.Request) (*http.Response, error) {
		close(requestDone)
		return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ackCalled, done := runSend(client, ctx)
	<-requestDone
	cancel()

	assertSendReturns(t, done)
	assertNoAck(t, ackCalled)
}

func newTestWebhookClient(rt roundTripperFunc) *WebhookClient {
	return &WebhookClient{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		webhookURL: "http://test.localhost/webhook",
		httpClient: &http.Client{Transport: rt},
	}
}

func runSend(client *WebhookClient, ctx context.Context) (*atomic.Bool, <-chan struct{}) {
	var ackCalled atomic.Bool
	done := make(chan struct{})
	go func() {
		client.Send(ctx, &OutboxEvt{ID: 1}, func(uint) { ackCalled.Store(true) })
		close(done)
	}()
	return &ackCalled, done
}

func assertSendReturns(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return after context cancellation")
	}
}

func assertNoAck(t *testing.T, ackCalled *atomic.Bool) {
	t.Helper()
	if ackCalled.Load() {
		t.Error("ackFn was called on cancelled context")
	}
}

// TestDIDWorkerOrdering tests DIDWorker ordering invariants directly via
// addEvent/ackEvent using testing/synctest for deterministic concurrency.
//
// Each round: add events → synctest.Wait → drain channel (checking invariants) → ack → synctest.Wait.
// Invariants checked on every received event:
//   - live-sends-alone: a live event is never sent while another is in-flight for its DID
//   - live-blocks-subsequent: no event is sent while a live event is in-flight for its DID
func TestDIDWorkerOrdering(t *testing.T) {
	type event struct {
		did  string
		live bool
	}
	type round struct {
		add    []int // event indices to addEvent
		expect []int // event indices expected in outgoing channel
		ack    []int // event indices to ackEvent
	}

	tests := []struct {
		name   string
		events []event
		rounds []round
	}{
		{
			name: "historical events sent concurrently",
			events: []event{
				{"alice", false},
				{"alice", false},
			},
			rounds: []round{
				{add: []int{0, 1}, expect: []int{0, 1}},
			},
		},
		{
			name: "live event waits for prior historical",
			events: []event{
				{"alice", false},
				{"alice", false},
				{"alice", true},
			},
			rounds: []round{
				{add: []int{0, 1, 2}, expect: []int{0, 1}, ack: []int{0, 1}},
				{expect: []int{2}},
			},
		},
		{
			name: "live blocks subsequent events",
			events: []event{
				{"alice", true},
				{"alice", false},
				{"alice", false},
			},
			rounds: []round{
				{add: []int{0, 1, 2}, expect: []int{0}, ack: []int{0}},
				{expect: []int{1, 2}},
			},
		},
		{
			name: "consecutive live events serialize",
			events: []event{
				{"alice", true},
				{"alice", true},
			},
			rounds: []round{
				{add: []int{0, 1}, expect: []int{0}, ack: []int{0}},
				{expect: []int{1}},
			},
		},
		{
			name: "out-of-order acks",
			events: []event{
				{"alice", false},
				{"alice", false},
				{"alice", false},
			},
			rounds: []round{
				{add: []int{0, 1}, expect: []int{0, 1}, ack: []int{1, 0}},
				{add: []int{2}, expect: []int{2}},
			},
		},
		{
			name: "different DIDs are independent",
			events: []event{
				{"alice", false},
				{"alice", true},
				{"bob", false},
				{"bob", true},
			},
			rounds: []round{
				{add: []int{0, 1, 2, 3}, expect: []int{0, 2}, ack: []int{0}},
				{expect: []int{1}, ack: []int{2}},
				{expect: []int{3}},
			},
		},
		{name: "outbox_buffer",
			events: []event{{"alice", false}, {"alice", true}, {"alice", false}},
			rounds: []round{
				{add: []int{0, 1, 2}, expect: []int{0}, ack: []int{0}},
				{expect: []int{1}, ack: []int{1}},
				{expect: []int{2}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				em := &EventManager{
					cache:      xsync.NewMap[uint, *OutboxEvt](),
					pendingIDs: make(chan uint, 100),
				}
				em.finishedLoading.Store(true)

				outgoing := make(chan *OutboxEvt, 100)
				ob := &Outbox{
					logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
					mode:       OutboxModeFireAndForget,
					events:     em,
					acks:       make(chan uint, 100),
					outgoing:   outgoing,
					didWorkers: xsync.NewMap[string, *DIDWorker](),
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer func() { cancel(); synctest.Wait() }()

				evts := make([]*OutboxEvt, len(tt.events))
				for i, e := range tt.events {
					evts[i] = &OutboxEvt{ID: uint(i + 1), Did: e.did, Live: e.live}
					em.cache.Store(evts[i].ID, evts[i])
				}

				workerFor := func(did string) *DIDWorker {
					return ob.workerFor(did)
				}

				inFlight := make(map[string]map[uint]bool)
				liveInFlight := make(map[string]uint)

				for ri, r := range tt.rounds {
					for _, idx := range r.add {
						workerFor(evts[idx].Did).addEvent(ctx, evts[idx])
					}
					synctest.Wait()

					got := make(map[uint]bool)
				drain:
					for {
						select {
						case evt := <-outgoing:
							did := evt.Did
							if inFlight[did] == nil {
								inFlight[did] = make(map[uint]bool)
							}
							if evt.Live && len(inFlight[did]) > 0 {
								t.Errorf("round %d: live event %d sent with %d in-flight for %s",
									ri, evt.ID, len(inFlight[did]), did)
							}
							if lid := liveInFlight[did]; lid != 0 {
								t.Errorf("round %d: event %d sent while live %d in-flight for %s",
									ri, evt.ID, lid, did)
							}
							inFlight[did][evt.ID] = true
							if evt.Live {
								liveInFlight[did] = evt.ID
							}
							got[evt.ID] = true
						default:
							break drain
						}
					}

					want := make(map[uint]bool, len(r.expect))
					for _, idx := range r.expect {
						want[evts[idx].ID] = true
					}
					if len(got) != len(want) {
						t.Fatalf("round %d: sent %v, want %v", ri, got, want)
					}
					for id := range want {
						if !got[id] {
							t.Fatalf("round %d: event %d not sent, got %v", ri, id, got)
						}
					}

					for _, idx := range r.ack {
						evt := evts[idx]
						delete(inFlight[evt.Did], evt.ID)
						if liveInFlight[evt.Did] == evt.ID {
							liveInFlight[evt.Did] = 0
						}
						workerFor(evt.Did).ackEvent(evt.ID)
					}
					synctest.Wait()
				}
			})
		})
	}
}
