package webbff

import (
 "errors"
 "net/http"
 "slices"

 "gitcode.com/urandon/sessionless/internal/domain"
 "gitcode.com/urandon/sessionless/internal/ports"
 "gitcode.com/urandon/sessionless/internal/webcontract"
)

// LoginProviderConfig is trusted construction DATA, not browser/provider input
// or a V2 installer. The explicit enum selects one exclusive concrete arm.
type LoginProviderConfig struct {
 Provider domain.IdentityProvider
 OAuthProvider ports.OAuthIdentityProvider
 OAuthClientID string
 OIDCProvider ports.OIDCProvider
 OIDCPolicy domain.OIDCVerificationPolicy
}

func validateLoginProviders(input []LoginProviderConfig)([]LoginProviderConfig,error){
 fail:=func()([]LoginProviderConfig,error){return nil,errors.New("login providers must be exactly yandex, telegram, or ordered yandex,telegram with complete exclusive bindings")}
 if len(input)<1||len(input)>2{return fail()}
 out:=slices.Clone(input)
 for i,p:=range out{
  if i==1&&(out[0].Provider!=domain.IdentityProviderYandex||p.Provider!=domain.IdentityProviderTelegram){return fail()}
  switch p.Provider{
  case domain.IdentityProviderYandex:
   if p.OAuthProvider==nil||domain.ValidateOpaqueID("oauth.client_id",p.OAuthClientID)!=nil||p.OIDCProvider!=nil||p.OIDCPolicy.Issuer!=""||p.OIDCPolicy.Audience!=""||len(p.OIDCPolicy.AllowedAlgorithms)!=0||p.OIDCPolicy.MaxClockSkew!=0{return fail()}
  case domain.IdentityProviderTelegram:
   if p.OIDCProvider==nil||p.OAuthProvider!=nil||p.OAuthClientID!=""||p.OIDCPolicy.Validate()!=nil{return fail()}
   out[i].OIDCPolicy.AllowedAlgorithms=slices.Clone(p.OIDCPolicy.AllowedAlgorithms)
  default:return fail()
  }
 };return out,nil
}
func(h *Handler)loginConfig(provider domain.IdentityProvider)(LoginProviderConfig,bool){for _,p:=range h.config.LoginProviders{if p.Provider==provider{return p,true}};return LoginProviderConfig{},false}
func loginCallbackProvider(path string)domain.IdentityProvider{switch path{case webcontract.RouteLoginCallback:return domain.IdentityProviderYandex;case webcontract.RouteOIDCCallback:return domain.IdentityProviderTelegram};return ""}
func loginCallbackPath(provider domain.IdentityProvider)string{if provider==domain.IdentityProviderYandex{return webcontract.RouteLoginCallback};if provider==domain.IdentityProviderTelegram{return webcontract.RouteOIDCCallback};return ""}
func(p LoginProviderConfig)issuer()string{if p.Provider==domain.IdentityProviderYandex{return domain.YandexOAuthIssuer};return p.OIDCPolicy.Issuer}
func(p LoginProviderConfig)audience()string{if p.Provider==domain.IdentityProviderYandex{return p.OAuthClientID};return p.OIDCPolicy.Audience}
func(h *Handler)loginMethods(w http.ResponseWriter,r *http.Request){
 if r.URL.RawQuery!=""||r.URL.ForceQuery||r.ContentLength!=0||len(r.TransferEncoding)!=0||r.Body!=nil&&r.Body!=http.NoBody{h.writeFailure(w,r,webcontract.ErrorInvalidRequest,"The request is invalid.");return}
 response:=webcontract.LoginMethods{Methods:make([]webcontract.LoginMethod,0,len(h.config.LoginProviders))}
 for _,p:=range h.config.LoginProviders{label,start:="Yandex",webcontract.RouteYandexLoginStart;if p.Provider==domain.IdentityProviderTelegram{label,start="Telegram",webcontract.RouteTelegramLoginStart};response.Methods=append(response.Methods,webcontract.LoginMethod{Provider:p.Provider,Label:label,StartPath:start})}
 h.writeJSON(w,http.StatusOK,response)
}
