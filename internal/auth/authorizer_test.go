package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/auth"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func TestAuthorizerEnforcesFeatureCachePriorityAndWildcard(t *testing.T) {
	tenant, _ := domain.NewTenantScope("tenant-a")
	wildcard, _ := domain.NewModelKey("*")
	streaming, _ := domain.NewFeatureSet(domain.FeatureVersion1, 1<<domain.FeatureStreaming)
	principal, err := domain.NewPrincipalFromParams(domain.PrincipalParams{Subject: "subject", Tenant: tenant, Models: []domain.ModelKey{wildcard}, Features: streaming, MaxPriority: domain.PriorityNormal, CachePolicies: []domain.CachePolicy{domain.CachePolicyDisabled}})
	if err != nil {
		t.Fatal(err)
	}
	request := inferenceRequest(t, "model-a@revision-1", streaming, domain.PriorityNormal, domain.CachePolicyDisabled)
	if authorized, err := (auth.Authorizer{}).Authorize(context.Background(), principal, request); err != nil || authorized.Request().Model() != request.Model() {
		t.Fatalf("wildcard authorization=%v err=%v", authorized, err)
	}
	for name, denied := range map[string]domain.InferenceRequest{
		"feature":  inferenceRequest(t, "model-a@revision-1", featureSet(t, domain.FeatureToolCalling), domain.PriorityNormal, domain.CachePolicyDisabled),
		"priority": inferenceRequest(t, "model-a@revision-1", streaming, domain.PriorityHigh, domain.CachePolicyDisabled),
		"cache":    inferenceRequest(t, "model-a@revision-1", streaming, domain.PriorityNormal, domain.CachePolicyPrefer),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (auth.Authorizer{}).Authorize(context.Background(), principal, denied); domain.ErrorKindOf(err) != domain.ErrorForbidden {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func featureSet(t *testing.T, feature domain.Feature) domain.FeatureSet {
	t.Helper()
	value, err := domain.NewFeatureSet(domain.FeatureVersion1, 1<<feature)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func inferenceRequest(t *testing.T, model string, features domain.FeatureSet, priority domain.Priority, cache domain.CachePolicy) domain.InferenceRequest {
	t.Helper()
	request, err := domain.NewInferenceRequest(domain.InferenceRequestParams{Model: model, Messages: []domain.MessageParams{{Role: "user", Content: "prompt"}}, MaxOutputTokens: 32, Priority: priority, Features: features, CachePolicy: cache, ExecutionBudget: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return request
}
