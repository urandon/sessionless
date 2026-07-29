package ydbclient

import "testing"

func TestDataDSNRemovesGooseOnlyFlags(t *testing.T) {
	got, err := DataDSN("grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare%2Cnumeric&keep=yes")
	if err != nil {
		t.Fatal(err)
	}
	want := "grpc://localhost:2136/local?keep=yes"
	if got != want {
		t.Fatalf("DataDSN() = %q, want %q", got, want)
	}
}
