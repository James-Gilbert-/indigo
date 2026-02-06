package main

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

// Ordering guarantees for events belonging to the same DID:
//
// Live events are synchronization barriers - all prior events must complete
// before a live event can be sent, and the live event must complete before
// any subsequent events can be sent.
//
// Historical events can be sent concurrently with each other (no ordering
// between them), but cannot be sent while a live event is in-flight.
//
// Example sequence: H1, H2, L1, L2, H3, H4, L3, H5
//   - H1 and H2 sent concurrently
//   - Wait for H1 and H2 to complete, then send L1 (alone)
//   - Wait for L1 to complete, then send L2 (alone)
//   - Wait for L2 to complete, then send H3 and H4 concurrently
//   - Wait for H3 and H4 to complete, then send L3 (alone)
//   - Wait for L3 to complete, then send H5

type Outbox struct {
	logger       *slog.Logger
	mode         OutboxMode
	parallelism  int
	retryTimeout time.Duration
	webhook      *WebhookClient

	events *EventManager

	didWorkers *xsync.Map[string, *DIDWorker]

	acks     chan uint
	outgoing chan *OutboxEvt
}

func NewOutbox(logger *slog.Logger, events *EventManager, config *TapConfig) *Outbox {
	return &Outbox{
		logger:       logger.With("component", "outbox"),
		mode:         parseOutboxMode(config.WebhookURL, config.DisableAcks),
		parallelism:  config.OutboxParallelism,
		retryTimeout: config.RetryTimeout,
		webhook: &WebhookClient{
			logger:        logger.With("component", "webhook_client"),
			webhookURL:    config.WebhookURL,
			adminPassword: config.AdminPassword,
			httpClient: &http.Client{
				Timeout: 30 * time.Second,
			},
		},
		events:     events,
		didWorkers: xsync.NewMap[string, *DIDWorker](),
		acks:       make(chan uint, config.OutboxParallelism*10000),
		outgoing:   make(chan *OutboxEvt, config.OutboxParallelism*10000),
	}
}

// Run starts the outbox workers for event delivery and cleanup.
func (o *Outbox) Run(ctx context.Context) {
	var wg sync.WaitGroup

	if o.mode == OutboxModeWebsocketAck {
		wg.Go(func() {
			ticker := time.NewTicker(o.retryTimeout)
			defer ticker.Stop()

			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
				case <-ticker.C:
					o.retryTimedOutEvents(ctx)
				}
			}
		})
	}

	for i := 0; i < o.parallelism; i++ {
		wg.Go(func() {
			o.runDelivery(ctx)
		})
	}

	for i := 0; i < o.parallelism; i++ {
		wg.Go(func() {
			o.runBatchedDeletes(ctx)
		})
	}

	wg.Wait()
}

func (o *Outbox) runDelivery(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case eventID := <-o.events.pendingIDs:
			if evt, exists := o.events.GetEvent(eventID); exists {
				o.workerFor(evt.Did).addEvent(ctx, evt)
			}
		}
	}
}

// workerFor gets or creates the DIDWorker for the given DID.
func (o *Outbox) workerFor(did string) *DIDWorker {
	w, _ := o.didWorkers.LoadOrCompute(did, func() (*DIDWorker, bool) {
		return &DIDWorker{
			did:       did,
			notifChan: make(chan struct{}, 1),
			inFlight:  make(map[uint]flightInfo),
			outbox:    o,
		}, false
	})
	return w
}

func (o *Outbox) sendEvent(ctx context.Context, evt *OutboxEvt) {
	eventsDelivered.Inc()
	switch o.mode {
	case OutboxModeFireAndForget, OutboxModeWebsocketAck:
		select {
		case o.outgoing <- evt:
		case <-ctx.Done():
		}
	case OutboxModeWebhook:
		go o.webhook.Send(ctx, evt, o.AckEvent)
	}
}

// AckEvent marks an event as delivered and queues it for deletion.
func (o *Outbox) AckEvent(eventID uint) {
	eventsAcked.Inc()
	if evt, exists := o.events.GetEvent(eventID); exists {
		if worker, ok := o.didWorkers.Load(evt.Did); ok {
			worker.ackEvent(eventID)
		}
	}
	o.acks <- eventID
}

func (o *Outbox) runBatchedDeletes(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var batch []uint
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := o.events.DeleteEvents(ctx, batch); err != nil {
			o.logger.Error("failed to delete batch of acked events", "error", err, "count", len(batch))
		}
		batch = nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case id := <-o.acks:
			batch = append(batch, id)
			if len(batch) >= 1000 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (o *Outbox) retryTimedOutEvents(ctx context.Context) {
	workers := make([]*DIDWorker, 0)

	o.didWorkers.Range(func(_ string, w *DIDWorker) bool {
		workers = append(workers, w)
		return ctx.Err() == nil
	})

	for _, w := range workers {
		for _, id := range w.timedOutEvents() {
			if evt, ok := o.events.GetEvent(id); ok {
				o.logger.Info("retrying timed out event", "id", id)
				o.sendEvent(ctx, evt)
			}
		}
		if ctx.Err() != nil {
			return
		}
	}
}

type flightInfo struct {
	sentAt time.Time
	isLive bool
}

type DIDWorker struct {
	outbox    *Outbox
	did       string
	notifChan chan struct{}

	// Protected by mu
	mu          sync.Mutex
	pendingEvts []uint
	inFlight    map[uint]flightInfo
	liveCnt     int // Number of Live events currently in flight
	running     bool
}

func (w *DIDWorker) run(ctx context.Context) {
	for {
		w.mu.Lock()
		for len(w.pendingEvts) > 0 && w.liveCnt == 0 {
			nextID := w.pendingEvts[0]
			evt, exists := w.outbox.events.GetEvent(nextID)
			if !exists {
				w.pendingEvts = w.pendingEvts[1:]
				continue
			}
			// Live event waits for all in-flight historical events to clear.
			if evt.Live && len(w.inFlight) > 0 {
				break
			}
			w.pendingEvts = w.pendingEvts[1:]
			w.startFlight(evt)
			w.mu.Unlock()
			w.outbox.sendEvent(ctx, evt)
			w.mu.Lock()
		}

		if w.isIdle() {
			w.running = false
			w.outbox.didWorkers.Delete(w.did)
			w.mu.Unlock()
			return
		}

		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-w.notifChan:
		}
	}
}

func (w *DIDWorker) addEvent(ctx context.Context, evt *OutboxEvt) {
	w.mu.Lock()

	// Resolve stale worker: between workerFor() and acquiring this lock, worker may need to re-register
	for {
		actual, _ := w.outbox.didWorkers.LoadOrCompute(w.did, func() (*DIDWorker, bool) {
			return w, false
		})
		if actual == w {
			break
		}
		w.mu.Unlock()
		w = actual
		w.mu.Lock()
	}

	// Fast path: send inline when idle.
	if w.isIdle() {
		w.startFlight(evt)
		w.mu.Unlock()
		w.outbox.sendEvent(ctx, evt)
		return
	}

	// Enqueue and ensure a drain goroutine is running.
	w.pendingEvts = append(w.pendingEvts, evt.ID)
	if !w.running {
		w.running = true
		go w.run(ctx)
	}
	w.mu.Unlock()
	w.signal()
}

func (w *DIDWorker) ackEvent(evtID uint) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if info, ok := w.inFlight[evtID]; ok {
		delete(w.inFlight, evtID)
		if info.isLive {
			w.liveCnt--
		}
	}
	if !w.running && w.isIdle() {
		w.outbox.didWorkers.Delete(w.did)
		return
	}
	w.signal()
}

// startFlight marks an event as in-flight. Must be called with mu held.
func (w *DIDWorker) startFlight(evt *OutboxEvt) {
	w.inFlight[evt.ID] = flightInfo{sentAt: time.Now(), isLive: evt.Live}
	if evt.Live {
		w.liveCnt++
	}
}

// isIdle reports whether the worker has no pending or in-flight events.
// Must be called with mu held.
func (w *DIDWorker) isIdle() bool {
	return len(w.pendingEvts) == 0 && len(w.inFlight) == 0
}

func (w *DIDWorker) timedOutEvents() []uint {
	w.mu.Lock()
	defer w.mu.Unlock()

	var timedOut []uint

	for evtId, info := range w.inFlight {
		if time.Since(info.sentAt) > w.outbox.retryTimeout {
			timedOut = append(timedOut, evtId)
			// Reset sentAt so we don't re-detect on the next tick
			info.sentAt = time.Now()
			w.inFlight[evtId] = info
		}
	}

	return timedOut
}

func (w *DIDWorker) signal() {
	select {
	case w.notifChan <- struct{}{}:
	default:
	}
}
