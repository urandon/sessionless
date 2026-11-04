package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// Private immutable catalog DATA, frozen once at the actual original MODEL
// producer after the same installed Source/Store/keeper/service composition.
// It is not readiness/current permission: every effect retains its own gates.
type localModelCatalogV3 struct {
	mcpDigest, searchDigest string
	declaration             ports.SearchCredentialDeclarationV1
	tools                   []configuredinference.ToolV1
}

func cloneLocalModelToolsV3(tools []configuredinference.ToolV1) []configuredinference.ToolV1 {
	copy := append([]configuredinference.ToolV1(nil), tools...)
	for i := range copy {
		copy[i].Parameters = bytes.Clone(copy[i].Parameters)
	}
	return copy
}

func (entry *LocalInvocationEntryV1) originalModelTools(ctx context.Context, profile domain.OwnedAgentProfileCurrentV1, grant mcpadmission.MCPToolGrantV1, at time.Time, operations []LocalCompactionOperationV1) ([]configuredinference.ToolV1, error) {
	deny := func() ([]configuredinference.ToolV1, error) { return nil, domain.ErrMembershipDenied }
	if !entry.original() || profile.Locator.Profile != domain.AgentLoopOwnedPublicV1 || ctx == nil || ctx.Err() != nil {
		return deny()
	}
	descriptor, err := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if err != nil {
		return deny()
	}
	tools := []configuredinference.ToolV1{{Name: descriptor.Name(), Description: grant.ModelDescription, Parameters: descriptor.InputSchema()}}
	tuple := entry.owner.ready.Load()
	if tuple == nil || tuple.store != entry.owner.store || tuple.bridge == nil || !sameLocalOwnerPointer(tuple.keeper, entry.owner.keeper) {
		clear(tools[0].Parameters)
		return deny()
	}
	if tuple.search == nil {
		if entry.modelCatalog != nil {
			clear(tools[0].Parameters)
			return deny()
		}
		return tools, nil // exact historical MCP-only composition/bytes
	}
	defer func() {
		for _, tool := range tools {
			clear(tool.Parameters)
		}
	}()
	if tuple.searchTransport == nil || tuple.search.ValidateConstructor() != nil || !entry.credentialExecutionActive() || chargeOwnedAuthorityBody(ctx, 4*ownedAuthorityHeadBytes) != nil {
		return deny()
	}
	declared, err := entry.owner.source.SearchCredentialConfiguration()
	if err != nil {
		return deny()
	}
	if declared == nil {
		if entry.modelCatalog != nil {
			return deny()
		}
		return cloneLocalModelToolsV3(tools), nil // authenticated omission
	}
	declaration, err := entry.owner.source.ReadSearchCredentialDeclaration(ctx, entry.source.Profile)
	if err != nil || declaration != *declared || profile.WebSearchResourceGrant == nil || descriptor.Name() != localFailureV4MCPToolName {
		return deny()
	}
	search, err := entry.owner.store.readOwnedWebSearchResourceGrant(ctx, profile, at)
	gd, ge := grant.Digest()
	if err != nil || search == nil || ge != nil || declaration.Resource.ResourceID != search.grant.CredentialRef || declaration.Audience != search.grant.CredentialAudience {
		return deny()
	}
	sd, err := search.grant.Digest()
	if err != nil {
		return deny()
	}
	tools = append(tools, websearch.ModelToolV1())
	if old := entry.modelCatalog; old != nil {
		if old.mcpDigest != gd || old.searchDigest != sd || old.declaration != declaration || !reflect.DeepEqual(old.tools, tools) {
			return deny()
		}
		return cloneLocalModelToolsV3(old.tools), nil
	}
	if len(operations) != 0 { // never introduce a catalog halfway through a run
		return deny()
	}
	entry.modelCatalog = &localModelCatalogV3{mcpDigest: gd, searchDigest: sd, declaration: declaration, tools: cloneLocalModelToolsV3(tools)}
	return cloneLocalModelToolsV3(entry.modelCatalog.tools), nil
}

func frozenLocalCatalogMatchesV3(sequence uint64, request []byte, tools []configuredinference.ToolV1) bool {
	catalog, err := compactioncontext.ReadModelCatalogV3(sequence, request)
	if err != nil {
		return false
	}
	defer clear(catalog.Tools)
	var wire []localCatalogToolV1
	if json.Unmarshal(catalog.Tools, &wire) != nil || len(wire) != len(tools) {
		return false
	}
	for i, tool := range wire {
		if tool.Type != "function" || tool.Function.Name != tools[i].Name || tool.Function.Description != tools[i].Description || !bytes.Equal(tool.Function.Parameters, tools[i].Parameters) {
			return false
		}
	}
	return true
}

// Shape-only extraction after the canonical catalog decoder, used to compare
// a used MCP slot with its full original normalized grant, never invert a hash.
type localCatalogToolV1 struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

func localSuccessCatalogsV3(profile string, ops []localRetainedTerminalOperationV1, catalogs []compactioncontext.ModelCatalogV3) error {
	index := 0
	for _, item := range ops {
		if item.Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
			continue
		}
		if index >= len(catalogs) || catalogs[index].Sequence != item.Reservation.Operation.Sequence {
			return domain.ErrMembershipDenied
		}
		var envelope struct {
			Limits domain.ConfiguredInferenceLimitsV1 `json:"limits"`
		}
		if json.Unmarshal(item.NormalizedBody, &envelope) != nil {
			return domain.ErrMembershipDenied
		}
		tools, err := compactioncontext.DecodeModelCatalogV3(catalogs[index].Tools, envelope.Limits)
		index++
		if err != nil {
			return domain.ErrMembershipDenied
		}
		model, err := configuredinference.DecodeSealedRetainedResponseV2(profile, item.Reservation, item.Observation, item.NormalizedBody)
		if err != nil {
			if model.Call != nil {
				clear(model.Call.Arguments)
			}
			return domain.ErrMembershipDenied
		}
		if model.Call != nil {
			found := false
			for _, tool := range tools {
				found = found || tool.Name == model.Call.Name
			}
			clear(model.Call.Arguments)
			if !found {
				return domain.ErrMembershipDenied
			}
		}
	}
	if index != len(catalogs) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Serving output preserves EVERY MODEL's actual immutable request catalog,
// including omitted versus empty. Historical decoding never consults a latest
// grant/catalog or reconstructs the erased request from its response.
func localOriginalModelCatalogsV3(configuration domain.ConfiguredInferenceProfileV1, ops []LocalCompactionOperationV1) ([]compactioncontext.ModelCatalogV3, error) {
	if len(ops) < 2 || len(ops) > 12 {
		return nil, domain.ErrMembershipDenied
	}
	var catalogs []compactioncontext.ModelCatalogV3
	for _, item := range ops {
		if item.Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
			continue
		}
		if !localTerminalMaterialBytesV3(item.Request, item.Reservation.Operation.RequestBytes, item.Reservation.Operation.RequestDigest) {
			return nil, domain.ErrMembershipDenied
		}
		catalog, err := compactioncontext.ReadModelCatalogV3(item.Reservation.Operation.Sequence, item.Request)
		if err != nil {
			return nil, domain.ErrMembershipDenied
		}
		if _, err := compactioncontext.DecodeModelCatalogV3(catalog.Tools, configuration.Limits); err != nil {
			return nil, domain.ErrMembershipDenied
		}
		catalogs = append(catalogs, catalog)
	}
	if len(catalogs) < 2 || len(catalogs) > 8 {
		return nil, domain.ErrMembershipDenied
	}
	return catalogs, nil
}
