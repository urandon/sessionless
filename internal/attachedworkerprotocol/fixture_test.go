package attachedworkerprotocol

import (
	"os"
	"path/filepath"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join("..", "..", "test", "fixtures", "attached-worker", "v1", name))
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestConformanceFixtures(t *testing.T) {
	valid, err := DecodeBatchV1(readFixture(t, "valid-heartbeat.json"))
	if err != nil || len(valid.Frames) != 1 {
		t.Fatalf("valid fixture: frames=%d err=%v", len(valid.Frames), err)
	}
	for _, name := range []string{"invalid-duplicate.json", "invalid-unknown.json", "malformed-trailing.json"} {
		_, err := DecodeBatchV1(readFixture(t, name))
		requireCode(t, err, ErrorMalformedFrame)
	}

	fixture := newProtocolFixture(t)
	machine, err := NewConformanceMachine(MachineConfig{Auth: fixture.auth, WorkerOffer: fixture.workerOffer,
		PlatformOffer: fixture.platformOffer, ImplementedVersions: []ProtocolVersion{1}})
	if err != nil {
		t.Fatal(err)
	}
	outOfOrder, err := DecodeBatchV1(readFixture(t, "out-of-order-drain.json"))
	if err != nil {
		t.Fatal(err)
	}
	requireCode(t, acceptAt(machine, DirectionPlatformToWorker, outOfOrder.Frames[0], fixture.auth.ChannelBinding, 1_800_000_000_000_000), ErrorProtocolViolation)
}
