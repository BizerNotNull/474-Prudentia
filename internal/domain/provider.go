package domain

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	CurrentManifestSchemaVersion uint16 = 1
	CurrentCapabilityVersion     uint16 = 1
	CurrentSignatureVersion      uint16 = 1
)

type ProviderCapability uint8
const (
	CapabilityInference ProviderCapability = iota + 1
	CapabilityMetrics
	CapabilityAPC
	CapabilityTermination
	CapabilityCacheMetadata
	CapabilityMover
)

type IdentityProfile uint8
const IdentityExactWorkloadMTLS IdentityProfile = 1

type APCIsolation uint8
const (
	APCDisabled APCIsolation = iota + 1
	APCTenantDedicated
	APCTenantSalted
)

type CapabilityManifestParams struct {
	ID string
	SchemaVersion, CapabilityVersion, SignatureVersion uint16
	SignatureVerified bool
	VerifiedAt, ValidFrom, ValidUntil time.Time
	ImageDigest, ProxyDigest string
	Routes, Fields []string
	Parser string
	IdentityProfile IdentityProfile
	APCIsolation APCIsolation
	TenantSaltVersion uint16
	TenantSaltProven bool
	Metrics []string
	Termination, CacheMetadata, Mover bool
}

type CapabilityManifest struct {
	id string
	schemaVersion, capabilityVersion, signatureVersion uint16
	verifiedAt, validFrom, validUntil time.Time
	imageDigest, proxyDigest string
	routes, fields []string
	parser string
	identityProfile IdentityProfile
	apcIsolation APCIsolation
	tenantSaltVersion uint16
	metrics []string
	termination, cacheMetadata, mover bool
}

func NewCapabilityManifest(p CapabilityManifestParams) (CapabilityManifest, error) {
	if !boundedProviderString(p.ID, 256) || p.SchemaVersion != CurrentManifestSchemaVersion || p.CapabilityVersion != CurrentCapabilityVersion || p.SignatureVersion != CurrentSignatureVersion || !p.SignatureVerified || p.VerifiedAt.IsZero() || p.ValidFrom.IsZero() || p.ValidUntil.IsZero() || p.ValidUntil.Before(p.ValidFrom) || p.VerifiedAt.Before(p.ValidFrom) || p.VerifiedAt.After(p.ValidUntil) || !validSHA256Digest(p.ImageDigest) || !validSHA256Digest(p.ProxyDigest) || p.IdentityProfile != IdentityExactWorkloadMTLS || !boundedProviderString(p.Parser, 128) {
		return CapabilityManifest{}, fmt.Errorf("invalid capability manifest")
	}
	routes, err := validatedUniqueStrings(p.Routes, 16, 256)
	if err != nil || !containsString(routes, "/v1/chat/completions") { return CapabilityManifest{}, fmt.Errorf("invalid capability manifest routes") }
	fields, err := validatedUniqueStrings(p.Fields, 64, 128)
	if err != nil { return CapabilityManifest{}, fmt.Errorf("invalid capability manifest fields") }
	metrics, err := validatedUniqueStringsAllowEmpty(p.Metrics, 64, 128)
	if err != nil { return CapabilityManifest{}, fmt.Errorf("invalid capability manifest metrics") }
	switch p.APCIsolation {
	case APCDisabled, APCTenantDedicated:
		if p.TenantSaltVersion != 0 || p.TenantSaltProven { return CapabilityManifest{}, fmt.Errorf("invalid APC isolation") }
	case APCTenantSalted:
		if p.TenantSaltVersion != 1 || !p.TenantSaltProven { return CapabilityManifest{}, fmt.Errorf("unproven tenant salt") }
	default:
		return CapabilityManifest{}, fmt.Errorf("unknown APC isolation")
	}
	return CapabilityManifest{id:p.ID, schemaVersion:p.SchemaVersion, capabilityVersion:p.CapabilityVersion, signatureVersion:p.SignatureVersion, verifiedAt:p.VerifiedAt, validFrom:p.ValidFrom, validUntil:p.ValidUntil, imageDigest:p.ImageDigest, proxyDigest:p.ProxyDigest, routes:routes, fields:fields, parser:p.Parser, identityProfile:p.IdentityProfile, apcIsolation:p.APCIsolation, tenantSaltVersion:p.TenantSaltVersion, metrics:metrics, termination:p.Termination, cacheMetadata:p.CacheMetadata, mover:p.Mover}, nil
}
func (m CapabilityManifest) ID() string { return m.id }
func (m CapabilityManifest) SchemaVersion() uint16 { return m.schemaVersion }
func (m CapabilityManifest) CapabilityVersion() uint16 { return m.capabilityVersion }
func (m CapabilityManifest) SignatureVersion() uint16 { return m.signatureVersion }
func (m CapabilityManifest) VerifiedAt() time.Time { return m.verifiedAt }
func (m CapabilityManifest) ValidFrom() time.Time { return m.validFrom }
func (m CapabilityManifest) ValidUntil() time.Time { return m.validUntil }
func (m CapabilityManifest) ImageDigest() string { return m.imageDigest }
func (m CapabilityManifest) ProxyDigest() string { return m.proxyDigest }
func (m CapabilityManifest) Routes() []string { return append([]string(nil), m.routes...) }
func (m CapabilityManifest) Fields() []string { return append([]string(nil), m.fields...) }
func (m CapabilityManifest) Parser() string { return m.parser }
func (m CapabilityManifest) IdentityProfile() IdentityProfile { return m.identityProfile }
func (m CapabilityManifest) APCIsolation() APCIsolation { return m.apcIsolation }
func (m CapabilityManifest) TenantSaltVersion() (uint16, bool) { return m.tenantSaltVersion, m.apcIsolation == APCTenantSalted }
func (m CapabilityManifest) Metrics() []string { return append([]string(nil), m.metrics...) }
func (m CapabilityManifest) Supports(c ProviderCapability) bool {
	if m.schemaVersion != CurrentManifestSchemaVersion || m.capabilityVersion != CurrentCapabilityVersion || m.signatureVersion != CurrentSignatureVersion { return false }
	switch c {
	case CapabilityInference: return true
	case CapabilityMetrics: return len(m.metrics) != 0
	case CapabilityAPC: return m.apcIsolation == APCTenantDedicated || (m.apcIsolation == APCTenantSalted && m.tenantSaltVersion == 1)
	case CapabilityTermination: return m.termination
	case CapabilityCacheMetadata: return m.cacheMetadata
	case CapabilityMover: return m.mover
	default: return false
	}
}
func (m CapabilityManifest) ValidAt(at time.Time) bool { return !at.IsZero() && !at.Before(m.validFrom) && !at.After(m.validUntil) && m.Supports(CapabilityInference) }
func (m CapabilityManifest) Compatible(o CapabilityManifest) bool { return m.id==o.id && m.schemaVersion==o.schemaVersion && m.capabilityVersion==o.capabilityVersion && m.imageDigest==o.imageDigest && m.proxyDigest==o.proxyDigest && m.parser==o.parser && m.identityProfile==o.identityProfile && equalStrings(m.routes,o.routes) && equalStrings(m.fields,o.fields) }
func (m CapabilityManifest) String() string { return "capability-manifest[verified]" }

type BackendCallParams struct { Request AuthorizedRequest; Target DispatchTarget; Manifest CapabilityManifest; ProviderRequestID string }
type BackendCall struct { request AuthorizedRequest; target DispatchTarget; manifest CapabilityManifest; providerRequestID string }
func NewBackendCall(p BackendCallParams) (BackendCall,error) { if !boundedProviderString(p.ProviderRequestID,256)||p.Request.Tenant()==""||p.Target.Endpoint()==""||!p.Manifest.Supports(CapabilityInference){return BackendCall{},fmt.Errorf("invalid backend call")}; return BackendCall{p.Request,p.Target,p.Manifest,p.ProviderRequestID},nil }
func (c BackendCall) Request() AuthorizedRequest{return c.request}; func(c BackendCall)Target()DispatchTarget{return c.target}; func(c BackendCall)Manifest()CapabilityManifest{return c.manifest}; func(c BackendCall)ProviderRequestID()string{return c.providerRequestID}

type ProbeTarget struct{target DispatchTarget;manifest CapabilityManifest}
func NewProbeTarget(target DispatchTarget,manifest CapabilityManifest)(ProbeTarget,error){if target.Endpoint()==""||!manifest.Supports(CapabilityInference){return ProbeTarget{},fmt.Errorf("invalid probe target")};return ProbeTarget{target,manifest},nil}
func(t ProbeTarget)Target()DispatchTarget{return t.target};func(t ProbeTarget)Manifest()CapabilityManifest{return t.manifest}

type RuntimeHealthState uint8
const(RuntimeHealthResponsive RuntimeHealthState=iota+1;RuntimeHealthUnresponsive;RuntimeHealthWarming)
type RuntimeHealthObservation struct{identity WorkloadIdentity;state RuntimeHealthState;observedAt time.Time}
func NewRuntimeHealthObservation(i WorkloadIdentity,s RuntimeHealthState,at time.Time)(RuntimeHealthObservation,error){if i.PodUID()==""||at.IsZero()||(s!=RuntimeHealthResponsive&&s!=RuntimeHealthUnresponsive&&s!=RuntimeHealthWarming){return RuntimeHealthObservation{},fmt.Errorf("invalid runtime health observation")};return RuntimeHealthObservation{i,s,at},nil}
func(o RuntimeHealthObservation)Identity()WorkloadIdentity{return o.identity};func(o RuntimeHealthObservation)State()RuntimeHealthState{return o.state};func(o RuntimeHealthObservation)ObservedAt()time.Time{return o.observedAt}

type LoadObservationParams struct{Identity WorkloadIdentity;ObservedAt time.Time;UsedSlots uint32;HasUsedSlots bool;QueueDepth uint32;HasQueueDepth bool}
type LoadObservation struct{identity WorkloadIdentity;observedAt time.Time;usedSlots uint32;hasUsedSlots bool;queueDepth uint32;hasQueueDepth bool}
func NewLoadObservation(p LoadObservationParams)(LoadObservation,error){if p.Identity.PodUID()==""||p.ObservedAt.IsZero()||(!p.HasUsedSlots&&p.UsedSlots!=0)||(!p.HasQueueDepth&&p.QueueDepth!=0){return LoadObservation{},fmt.Errorf("invalid load observation")};return LoadObservation{p.Identity,p.ObservedAt,p.UsedSlots,p.HasUsedSlots,p.QueueDepth,p.HasQueueDepth},nil}
func(o LoadObservation)Identity()WorkloadIdentity{return o.identity};func(o LoadObservation)ObservedAt()time.Time{return o.observedAt};func(o LoadObservation)UsedSlots()(uint32,bool){return o.usedSlots,o.hasUsedSlots};func(o LoadObservation)QueueDepth()(uint32,bool){return o.queueDepth,o.hasQueueDepth}

type ProviderRequestRefParams struct{Tenant,RequestID,ProviderRequestID string;Target WorkloadIdentity;Manifest CapabilityManifest;ExpiresAt time.Time}
type ProviderRequestRef struct{tenant,requestID,providerRequestID string;target WorkloadIdentity;manifest CapabilityManifest;expiresAt time.Time}
func NewProviderRequestRef(p ProviderRequestRefParams)(ProviderRequestRef,error){if !boundedProviderString(p.Tenant,128)||!boundedProviderString(p.RequestID,256)||!boundedProviderString(p.ProviderRequestID,256)||p.Target.PodUID()==""||p.ExpiresAt.IsZero()||!p.Manifest.Supports(CapabilityTermination){return ProviderRequestRef{},fmt.Errorf("provider termination unsupported or invalid")};return ProviderRequestRef{p.Tenant,p.RequestID,p.ProviderRequestID,p.Target,p.Manifest,p.ExpiresAt},nil}
func(r ProviderRequestRef)Tenant()string{return r.tenant};func(r ProviderRequestRef)RequestID()string{return r.requestID};func(r ProviderRequestRef)ProviderRequestID()string{return r.providerRequestID};func(r ProviderRequestRef)Target()WorkloadIdentity{return r.target};func(r ProviderRequestRef)Manifest()CapabilityManifest{return r.manifest};func(r ProviderRequestRef)ExpiresAt()time.Time{return r.expiresAt};func(r ProviderRequestRef)String()string{return "provider-request[redacted]"}

func validSHA256Digest(v string)bool{if !strings.HasPrefix(v,"sha256:")||len(v)!=71{return false};_,err:=hex.DecodeString(v[7:]);return err==nil}
func boundedProviderString(v string,max int)bool{return v!=""&&len(v)<=max&&v==strings.TrimSpace(v)}
func validatedUniqueStrings(in []string,maxCount,maxLen int)([]string,error){if len(in)==0{return nil,fmt.Errorf("empty")};return validatedUniqueStringsAllowEmpty(in,maxCount,maxLen)}
func validatedUniqueStringsAllowEmpty(in []string,maxCount,maxLen int)([]string,error){if len(in)>maxCount{return nil,fmt.Errorf("too many")};out:=append([]string(nil),in...);seen:=make(map[string]struct{},len(out));for _,v:=range out{if !boundedProviderString(v,maxLen){return nil,fmt.Errorf("invalid")};if _,ok:=seen[v];ok{return nil,fmt.Errorf("duplicate")};seen[v]=struct{}{}};return out,nil}
func containsString(v []string,w string)bool{for _,s:=range v{if s==w{return true}};return false};func equalStrings(a,b []string)bool{if len(a)!=len(b){return false};for i:=range a{if a[i]!=b[i]{return false}};return true}
