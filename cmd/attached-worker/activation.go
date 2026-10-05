package main

import (
	"context"
	"errors"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeractivation"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerservice"
)

// The opt-in command paths retain the same exact local runtime lease held by
// the authenticated synthetic owner. Absent an explicit private profile, the
// old feature-disabled paths in main.go remain unchanged.
func runActivated(parent context.Context, store *attachedworkerlocal.Store, path, expectedDigest string, output io.Writer) int {
	return runActivatedWithConnector(parent, store, path, expectedDigest, output, attachedworkeractivation.Connect)
}

type activationConnector func(context.Context, *attachedworkerlocal.Store, attachedworkeractivation.ProfileV1) (*attachedworkersealedinput.SyntheticRuntime, error)

func runActivatedWithConnector(parent context.Context, store *attachedworkerlocal.Store, path, expectedDigest string, output io.Writer, connector activationConnector) int {
	profile, digest, err := attachedworkeractivation.ReadProfileWithDigest(path)
	if connector == nil || err != nil || expectedDigest != "" && digest != expectedDigest {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	runtime, activeProfile, err := connectActivatedWith(parent, store, profile, connector)
	if err != nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeIO}, 1)
	}
	owner, err := attachedworkeractivation.NewOwner(runtime, activeProfile)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		_ = runtime.Close(cleanupCtx)
		cancel()
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	runErr := owner.Run(parent)
	if parent.Err() != nil && errors.Is(runErr, context.Canceled) {
		runErr = nil
	}
	return writeOperation(output, owner.Result(), runErr)
}

func serveActivated(parent context.Context, store *attachedworkerlocal.Store, stateRoot, path, expectedDigest string, expectedRevision uint64, output io.Writer) int {
	return serveActivatedWithConnector(parent, store, stateRoot, path, expectedDigest, expectedRevision, output, attachedworkeractivation.Connect)
}

func serveActivatedWithConnector(parent context.Context, store *attachedworkerlocal.Store, stateRoot, path, expectedDigest string, expectedRevision uint64, output io.Writer, connector activationConnector) int {
	profile, digest, err := attachedworkeractivation.ReadProfileWithDigest(path)
	if err != nil || digest != expectedDigest {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	if profile.ManifestRevision != expectedRevision {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeConflict}, 1)
	}
	controlDir, err := attachedworkerservice.Directory(stateRoot)
	if err != nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	runtime, activeProfile, err := connectActivatedWith(parent, store, profile, connector)
	if err != nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeIO}, 1)
	}
	owner, err := attachedworkeractivation.NewOwner(runtime, activeProfile)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		_ = runtime.Close(cleanupCtx)
		cancel()
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	serveCtx, cancelServe := context.WithCancel(parent)
	defer cancelServe()
	runDone := make(chan error, 1)
	go func() {
		runDone <- owner.Run(serveCtx)
		cancelServe()
	}()
	readyCtx, cancelReady := context.WithTimeout(serveCtx, commandTimeout)
	readyErr := owner.WaitReady(readyCtx)
	cancelReady()
	var serveErr error
	if readyErr == nil {
		serveErr = attachedworkerservice.Serve(serveCtx, controlDir, owner, store.Doctor)
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), commandTimeout)
	closeErr := owner.Shutdown(cleanupCtx)
	cancelCleanup()
	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(commandTimeout):
		runErr = context.DeadlineExceeded
	}
	if errors.Is(runErr, context.Canceled) && serveCtx.Err() != nil {
		runErr = nil
	}
	return writeOperation(output, owner.Result(), errors.Join(readyErr, serveErr, closeErr, runErr))
}

func connectActivated(parent context.Context, store *attachedworkerlocal.Store, profile attachedworkeractivation.ProfileV1) (*attachedworkersealedinput.SyntheticRuntime, attachedworkeractivation.ProfileV1, error) {
	return connectActivatedWith(parent, store, profile, attachedworkeractivation.Connect)
}

func connectActivatedWith(parent context.Context, store *attachedworkerlocal.Store, profile attachedworkeractivation.ProfileV1, connector activationConnector) (*attachedworkersealedinput.SyntheticRuntime, attachedworkeractivation.ProfileV1, error) {
	before, err := store.LoadSnapshot(parent)
	if connector == nil || err != nil || attachedworkeractivation.ValidateForManifest(profile, before.Manifest) != nil ||
		before.Manifest.Revision == ^uint64(0) || before.Manifest.ConnectionGeneration == ^uint64(0) {
		return nil, attachedworkeractivation.ProfileV1{}, attachedworkeractivation.ErrInvalidProfile
	}
	runtime, err := connector(parent, store, profile)
	if err != nil {
		return nil, attachedworkeractivation.ProfileV1{}, err
	}
	snapshot, err := store.LoadSnapshot(parent)
	if err != nil || snapshot.Manifest.Revision != before.Manifest.Revision+1 ||
		snapshot.Manifest.ConnectionGeneration != before.Manifest.ConnectionGeneration+1 {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		_ = runtime.Close(cleanupCtx)
		cancel()
		return nil, attachedworkeractivation.ProfileV1{}, attachedworkeractivation.ErrInvalidProfile
	}
	active := profile
	active.ManifestRevision = snapshot.Manifest.Revision
	active.ConnectionGeneration = snapshot.Manifest.ConnectionGeneration
	return runtime, active, nil
}
