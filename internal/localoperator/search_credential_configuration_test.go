package localoperator_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/localoperator"
)

func TestSearchCredentialConfigurationDistinguishesOriginalOmission(t *testing.T) {
	for _, present := range []bool{false, true} {
		name := "omitted"
		if present {
			name = "configured"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root, executable, manifest, grant := searchSourceFiles(t)
				want := sourceSearchCredential(manifest, grant)
				if present {
					manifest.SearchCredential = &want
					writeFitManifest(t, root, manifest)
				}
				source := openFitSource(t, t.Context(), root, executable)
				if got, err := source.SearchCredentialConfiguration(); err == nil || got != nil {
					t.Fatal("unaccepted Source became authenticated omission/configuration", err)
				}
				acceptSearchSourceTestPostimage(t, source)
				got, err := source.SearchCredentialConfiguration()
				if err != nil || (got != nil) != present {
					t.Fatalf("authenticated configuration presence=%t want=%t err=%v", got != nil, present, err)
				}
				if present {
					if *got != want {
						t.Fatal("configuration substituted the original credential identity")
					}
					got.Resource.Revision++
					again, err := source.SearchCredentialConfiguration()
					if err != nil || again == nil || *again != want {
						t.Fatal("constructor DATA aliases original Source", err)
					}
				}
				if err := source.Close(); err != nil {
					t.Fatal(err)
				}
				if got, err := source.SearchCredentialConfiguration(); err == nil || got != nil {
					t.Fatal("closed Source became authenticated omission", err)
				}
			})
		})
	}
}

func TestSearchCredentialConfigurationRejectsAbsentAndExpiredSource(t *testing.T) {
	for _, source := range []*localoperator.Source{nil, new(localoperator.Source)} {
		if got, err := source.SearchCredentialConfiguration(); err == nil || got != nil {
			t.Fatal("absent original Source became omission", err)
		}
	}
	synctest.Test(t, func(t *testing.T) {
		root, executable, _, _ := searchSourceFiles(t)
		parent, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		source := openFitSource(t, parent, root, executable)
		acceptSearchSourceTestPostimage(t, source)
		if got, err := source.SearchCredentialConfiguration(); err != nil || got != nil {
			t.Fatal("valid omitted configuration prerequisite", err)
		}
		time.Sleep(time.Minute) // virtual time inside this isolated synctest bubble
		if got, err := source.SearchCredentialConfiguration(); err == nil || got != nil {
			t.Fatal("expired Source became authenticated omission", err)
		}
	})
}
