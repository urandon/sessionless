package controlapi

import (
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// AttachedWorkerOptions composes existing owner-fenced routes without opening
// dependencies. The caller owns activation and dependency lifecycle. Normal
// sealed-input composition is credentialless; test-provider constructors are
// deliberately not available here, and real provider activation remains #133.
func AttachedWorkerOptions(audience string, state *ydbstore.Store, blobs ports.BlobStore) (Options, error) {
	return AttachedWorkerOptionsWithSignals(audience, state, blobs, nil)
}

// AttachedWorkerOptionsWithSignals shares the process-owned best-effort sink.
// Observations do not activate a worker profile or acquire new dependencies.
func AttachedWorkerOptionsWithSignals(audience string, state *ydbstore.Store, blobs ports.BlobStore, signals opssignal.Sink) (Options, error) {
	if state == nil || blobs == nil {
		return Options{}, fmt.Errorf("attached worker control dependencies are required")
	}
	service, err := attachedworkertransport.NewReceiptFinalizingService(attachedworkertransport.ServiceConfig{
		OpsSignals: signals,
		IDs:        idgen.New(), Audience: audience,
		PlatformOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		ChallengeLifetime:   5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval,
	}, state, state)
	if err != nil {
		return Options{}, fmt.Errorf("compose attached worker transport: %w", err)
	}
	bootstrap, err := attachedworkerhttp.NewBootstrapHandler(service)
	if err != nil {
		return Options{}, err
	}
	adapter, err := attachedworkerhttp.NewCoreExchangeAdapter(service)
	if err != nil {
		return Options{}, err
	}
	exchange, err := attachedworkerhttp.NewHandler(adapter)
	if err != nil {
		return Options{}, err
	}
	sealed, err := attachedworkersealedinput.NewService(service, state, blobs)
	if err != nil {
		return Options{}, err
	}
	receipt, err := attachedworkerreceipt.NewServiceWithOpsSignals(service, state, blobs, signals)
	if err != nil {
		return Options{}, err
	}
	inline, err := attachedworkerreceipt.NewInlineServiceV2(service, state, blobs)
	if err != nil {
		return Options{}, err
	}
	return Options{
		AttachedWorkerBootstrap: bootstrap, AttachedWorkerExchange: exchange,
		AttachedWorkerSealedInput:    attachedworkersealedinput.Handler(sealed),
		AttachedWorkerOutputReceipt:  attachedworkerreceipt.Handler(receipt),
		AttachedWorkerInlineOutputV2: attachedworkerreceipt.HandlerInlineV2(inline),
	}, nil
}
