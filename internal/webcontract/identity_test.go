package webcontract_test

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func TestIdentityAccountStartClosedSelectors(t *testing.T) {
	for _, request := range []webcontract.IdentityOperationStartRequest{
		{Kind:domain.IdentityOperationLinkV2, TargetProvider:domain.IdentityProviderTelegram, ExpectedRevision:1},
		{Kind:domain.IdentityOperationUnlinkV2, MethodHandle:"own-method-handle", ExpectedRevision:1},
	} { if err:=request.Validate();err!=nil {t.Fatalf("valid %s selector: %v",request.Kind,err)} }
	for _, request := range []webcontract.IdentityOperationStartRequest{
		{Kind:domain.IdentityOperationLinkV2, TargetProvider:domain.IdentityProviderTelegram},
		{Kind:domain.IdentityOperationLinkV2, TargetProvider:"other",ExpectedRevision:1},
		{Kind:domain.IdentityOperationLinkV2, TargetProvider:domain.IdentityProviderTelegram,MethodHandle:"other",ExpectedRevision:1},
		{Kind:domain.IdentityOperationUnlinkV2,TargetProvider:domain.IdentityProviderTelegram,MethodHandle:"own",ExpectedRevision:1},
		{Kind:domain.IdentityOperationUnlinkV2,ExpectedRevision:1},
		{Kind:"merge",ExpectedRevision:1},
	} { if request.Validate()==nil {t.Fatalf("invalid selector accepted: %+v",request)} }
}

func TestIdentityAccountRevisionsStayLosslessStrings(t *testing.T) {
	for _, value:=range []uint64{1,1<<53-1,1<<53,1<<53+1,math.MaxUint64} {
		raw:=`{"kind":"link","target_provider":"telegram","expected_revision":"`+strconv.FormatUint(value,10)+`"}`
		var request webcontract.IdentityOperationStartRequest
		if err:=json.Unmarshal([]byte(raw),&request);err!=nil||request.Validate()!=nil||uint64(request.ExpectedRevision)!=value {t.Fatalf("lossless request %d: %v",value,err)}
		revision:=domain.AuthRevisionV2(value)
		encoded,err:=json.Marshal(webcontract.IdentityOperationView{OperationID:"opaque-operation",BeforeRevision:&revision,AfterRevision:&revision})
		if err!=nil||!strings.Contains(string(encoded),`"after_revision":"`+strconv.FormatUint(value,10)+`"`) {t.Fatalf("lossless view %d: %s err=%v",value,encoded,err)}
	}
	for _, revision:=range []string{`1`,`0`,`"0"`,`"01"`,`"+1"`,`"1.0"`,`"1e3"`,`"18446744073709551616"`,`null`} {
		var request webcontract.IdentityOperationRevisionRequest
		if err:=json.Unmarshal([]byte(`{"expected_revision":`+revision+`}`),&request);err==nil&&request.Validate()==nil {t.Fatalf("revision alias accepted: %s",revision)}
	}
}

func TestIdentityAccountPublicViewsOmitPrivateAuthority(t *testing.T) {
	revision:=domain.AuthRevisionV2(7)
	encoded,err:=json.Marshal(webcontract.AccountLoginMethods{Revision:revision,Methods:[]webcontract.AccountLoginMethod{{Provider:domain.IdentityProviderYandex,MethodHandle:"opaque-handle",Linked:true,Current:true,Enabled:true,AllowedActions:[]string{"unlink"},RemainingProvider:domain.IdentityProviderTelegram}}})
	if err!=nil {t.Fatal(err)}
	for _, forbidden:=range []string{`"subject"`,`"user_id"`,`"csrf"`,`"pkce"`,`"nonce"`,`"session_digest"`,`"proof"`} {if strings.Contains(string(encoded),forbidden) {t.Fatalf("private field %s in public methods",forbidden)}}
	view,err:=json.Marshal(webcontract.IdentityOperationView{OperationID:"opaque-operation",Outcome:domain.IdentityOutcomeUnavailableV2})
	if err!=nil||strings.Contains(string(view),"revision")||strings.Contains(string(view),"phase")||strings.Contains(string(view),"completed_at") {t.Fatalf("unknown outcome fabricated facts: %s err=%v",view,err)}
}
