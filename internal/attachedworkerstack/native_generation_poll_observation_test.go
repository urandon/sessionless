package attachedworkerstack_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
)

// Test-only wire observations, not a private metadata ACK or execution grant.
// Kept untagged so the ordinary Go suite exercises the real idle HTTP contract.
type joinedGenerationPollObservation struct {
	lastGenerationPublishPath int
	exchangesAfterPublication uint64
}

func (o *joinedGenerationPollObservation) observePollResponse(pathCount int, path string, status int) {
	switch path {
	case attachedworkerhttp.ResourceGenerationPublishPathV1:
		if status != http.StatusOK {
			return
		}
		o.lastGenerationPublishPath = pathCount
		o.exchangesAfterPublication = 0
	case attachedworkerhttp.ExchangePathV1:
		// The real exchange handler/client use 204 for a successful idle
		// heartbeat, and 200 only when a protocol batch is returned.
		if (status == http.StatusOK || status == http.StatusNoContent) && o.lastGenerationPublishPath > 0 && pathCount > o.lastGenerationPublishPath {
			o.exchangesAfterPublication++
		}
	}
}

type pollObservationIdleService struct{ calls int }

func (s *pollObservationIdleService) Exchange(_ context.Context, bearer attachedworkerhttp.BearerToken, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	s.calls++
	return nil, nil
}

type pollObservationWire struct {
	handler           http.Handler
	observed          *joinedGenerationPollObservation
	paths, lastStatus int
}

func (w *pollObservationWire) RoundTrip(r *http.Request) (*http.Response, error) {
	defer r.Body.Close()
	w.paths++
	recorder := httptest.NewRecorder()
	w.handler.ServeHTTP(recorder, r)
	w.lastStatus = recorder.Code
	w.observed.observePollResponse(w.paths, r.URL.Path, recorder.Code)
	return recorder.Result(), nil
}

func TestJoinedGenerationFreshPollObservationKeepsStatusAndOrderFences(t *testing.T) {
	var observed joinedGenerationPollObservation
	for _, step := range []struct {
		name, path       string
		position, status int
		publish          int
		exchanges        uint64
	}{
		{"idle before publication", attachedworkerhttp.ExchangePathV1, 1, http.StatusNoContent, 0, 0},
		{"204 is not metadata success", attachedworkerhttp.ResourceGenerationPublishPathV1, 2, http.StatusNoContent, 0, 0},
		{"metadata denied", attachedworkerhttp.ResourceGenerationPublishPathV1, 3, http.StatusUnauthorized, 0, 0},
		{"actual publication success", attachedworkerhttp.ResourceGenerationPublishPathV1, 4, http.StatusOK, 4, 0},
		{"old exchange position", attachedworkerhttp.ExchangePathV1, 4, http.StatusOK, 4, 0},
		{"metadata read is not publication", attachedworkerhttp.ResourceGenerationReadPathV1, 5, http.StatusOK, 4, 0},
		{"exchange denied", attachedworkerhttp.ExchangePathV1, 6, http.StatusForbidden, 4, 0},
		{"exchange unavailable", attachedworkerhttp.ExchangePathV1, 7, http.StatusServiceUnavailable, 4, 0},
		{"unknown successful status", attachedworkerhttp.ExchangePathV1, 8, http.StatusAccepted, 4, 0},
		{"batch returned", attachedworkerhttp.ExchangePathV1, 9, http.StatusOK, 4, 1},
		{"idle returned", attachedworkerhttp.ExchangePathV1, 10, http.StatusNoContent, 4, 2},
		{"new publication resets observations", attachedworkerhttp.ResourceGenerationPublishPathV1, 11, http.StatusOK, 11, 0},
	} {
		observed.observePollResponse(step.position, step.path, step.status)
		if observed.lastGenerationPublishPath != step.publish || observed.exchangesAfterPublication != step.exchanges {
			t.Fatalf("%s: publish=%d exchanges=%d want=%d/%d", step.name, observed.lastGenerationPublishPath, observed.exchangesAfterPublication, step.publish, step.exchanges)
		}
	}
}

func TestJoinedGenerationFreshPollObservationCountsRealIdleExchange(t *testing.T) {
	service := &pollObservationIdleService{}
	handler, err := attachedworkerhttp.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	observed := &joinedGenerationPollObservation{}
	// The joined fixture has observed successful successor publication before
	// its idle heartbeat. This is DATA only; the production owner grants ACK.
	observed.observePollResponse(1, attachedworkerhttp.ResourceGenerationPublishPathV1, http.StatusOK)
	wire := &pollObservationWire{handler: handler, observed: observed, paths: 1}
	client, err := attachedworkerhttp.NewClientFromBearerBytes(attachedworkerhttp.ClientBytesConfig{BaseURL: "https://poll-observer.invalid", Bearer: []byte("synthetic-poll-observer"), HTTPClient: &http.Client{Transport: wire}, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	for sequence := uint64(1); sequence <= 2; sequence++ {
		batch := attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence), WorkerID: "poll-worker", EnrollmentGeneration: 1, ConnectionGeneration: 1, Sequence: sequence, Kind: attachedworkerprotocol.MessageHeartbeat, Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: 1, Available: true}}}}
		response, err := client.Exchange(ctx, batch)
		if err != nil || response != nil || wire.lastStatus != http.StatusNoContent || service.calls != int(sequence) {
			t.Fatalf("real handler/client idle poll failed: sequence=%d status=%d calls=%d err=%v", sequence, wire.lastStatus, service.calls, err)
		}
		if observed.exchangesAfterPublication != sequence {
			t.Errorf("successful idle poll invisible to observer: got=%d want=%d", observed.exchangesAfterPublication, sequence)
		}
	}
}
