package vllm

import "net/http"

const (
	chatCompletionsMethod = http.MethodPost
	chatCompletionsPath   = "/v1/chat/completions"
	terminationMethod     = http.MethodPost
	terminationPath       = "/v1/prudentia/terminate"
	healthMethod          = http.MethodGet
	healthPath            = "/health"
	metricsMethod         = http.MethodGet
	metricsPath           = "/metrics"
	modelsMethod          = http.MethodGet
	modelsPath            = "/v1/models"
)

func allowedProxyRoute(method, path string) bool {
	switch path {
	case chatCompletionsPath:
		return method == chatCompletionsMethod
	case terminationPath:
		return method == terminationMethod
	case healthPath:
		return method == healthMethod
	case metricsPath:
		return method == metricsMethod
	case modelsPath:
		return method == modelsMethod
	default:
		return false
	}
}
