package main

import (
 "slices"
 "testing"
 "gitcode.com/urandon/sessionless/internal/domain"
)

func loginConfigLookup(env map[string]string) func(string) (string, bool) {
 return func(name string) (string,bool) { value,ok:=env[name];return value,ok }
}
func completeLoginEnvironment(mode string) map[string]string {
 return map[string]string{"WEB_LOGIN_PROVIDERS":mode,"YANDEX_LOGIN_CLIENT_ID":"client-test","YANDEX_LOGIN_CLIENT_SECRET":"synthetic-yandex-secret","TELEGRAM_OIDC_CLIENT_ID":"123456","TELEGRAM_OIDC_CLIENT_SECRET":"synthetic-telegram-secret"}
}
func TestWebLoginConfigurationThreeClosedModes(t *testing.T) {
 for _,test:=range []struct{mode string;want []domain.IdentityProvider}{
  {"yandex",[]domain.IdentityProvider{domain.IdentityProviderYandex}},
  {"telegram",[]domain.IdentityProvider{domain.IdentityProviderTelegram}},
  {"yandex,telegram",[]domain.IdentityProvider{domain.IdentityProviderYandex,domain.IdentityProviderTelegram}},
 } { t.Run(test.mode,func(t *testing.T){
  env:=completeLoginEnvironment(test.mode)
  if test.mode=="yandex" { delete(env,"TELEGRAM_OIDC_CLIENT_ID");delete(env,"TELEGRAM_OIDC_CLIENT_SECRET") }
  if test.mode=="telegram" { delete(env,"YANDEX_LOGIN_CLIENT_ID");delete(env,"YANDEX_LOGIN_CLIENT_SECRET") }
  config,err:=webLoginFromEnvironment(loginConfigLookup(env),"https://web.example.invalid",false)
  if err!=nil { t.Fatal("complete selected configuration",err) }
  var actual []domain.IdentityProvider
  for _,p:=range config.providers {
   actual=append(actual,p.Provider)
   if p.Provider==domain.IdentityProviderYandex {
    if p.OAuthProvider==nil||p.OAuthClientID!="client-test"||p.OIDCProvider!=nil {t.Fatal("mixed/incomplete Yandex arm")}
   }else if p.OIDCProvider==nil||p.OIDCPolicy.Audience!="123456"||p.OAuthProvider!=nil||p.OIDCPolicy.Validate()!=nil {t.Fatal("mixed/incomplete Telegram arm")}
  }
  if !slices.Equal(actual,test.want) {t.Fatalf("providers=%v want=%v",actual,test.want)}
 }) }
}
func TestWebLoginConfigurationRejectsAliasesAndUnclosedModes(t *testing.T) {
 for _,mode:=range []string{"","telegram,yandex","yandex,yandex","telegram,telegram","yandex,telegram,telegram","unknown"," yandex","telegram,"} {
  t.Run(mode,func(t *testing.T){c,e:=webLoginFromEnvironment(loginConfigLookup(completeLoginEnvironment(mode)),"https://web.example.invalid",false);if e==nil||len(c.providers)!=0 {t.Fatal("unclosed configuration produced adapters")}})
 }
 for _,old:=range []string{"","telegram","yandex"} {
  env:=completeLoginEnvironment("yandex,telegram");env["WEB_LOGIN_PROVIDER"]=old
  c,e:=webLoginFromEnvironment(loginConfigLookup(env),"https://web.example.invalid",false);if e==nil||len(c.providers)!=0 {t.Fatal("singular variable accepted, including explicit empty")}
 }
 env:=completeLoginEnvironment("telegram");delete(env,"WEB_LOGIN_PROVIDERS")
 if _,e:=webLoginFromEnvironment(loginConfigLookup(env),"https://web.example.invalid",false);e==nil {t.Fatal("missing mode defaulted")}
}
func TestWebLoginConfigurationMissingSelectedCredentialNeverFallsBack(t *testing.T) {
 for _,field:=range []string{"YANDEX_LOGIN_CLIENT_ID","YANDEX_LOGIN_CLIENT_SECRET","TELEGRAM_OIDC_CLIENT_ID","TELEGRAM_OIDC_CLIENT_SECRET"} {
  t.Run(field,func(t *testing.T){
   env:=completeLoginEnvironment("yandex,telegram")
   if _,e:=webLoginFromEnvironment(loginConfigLookup(env),"https://web.example.invalid",false);e!=nil {t.Fatal("complete positive control",e)}
   delete(env,field)
   c,e:=webLoginFromEnvironment(loginConfigLookup(env),"https://web.example.invalid",false);if e==nil||len(c.providers)!=0 {t.Fatal("partial/missing selected configuration fell back")}
  })
 }
}
func TestYandexConfigurationRejectsNonLocalEndpointOverrides(t *testing.T) {
 env:=completeLoginEnvironment("yandex");env["YANDEX_LOGIN_TOKEN_ENDPOINT"]="http://127.0.0.1:8000/token"
 if _,e:=webLoginFromEnvironment(loginConfigLookup(env),"https://web.example.invalid",false);e==nil {t.Fatal("cloud accepted fixture endpoint")}
 if _,e:=webLoginFromEnvironment(loginConfigLookup(env),"https://web.example.invalid",true);e!=nil {t.Fatal("explicit local fixture refused",e)}
}
