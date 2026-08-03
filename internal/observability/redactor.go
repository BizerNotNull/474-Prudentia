package observability

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

type Redactor struct {
	limit        int
	credential   *regexp.Regexp
	jwt          *regexp.Regexp
	address      *regexp.Regexp
	secretFields map[string]struct{}
}

func NewRedactor(limit int) (*Redactor, error) {
	if limit <= 0 {
		return nil, &redactorError{"positive diagnostic limit is required"}
	}
	return &Redactor{
		limit:      limit,
		credential: regexp.MustCompile(`(?i)(authorization|api[-_]?key|token|capability|ticket)\s*[:=]\s*[^\s,;]+`),
		jwt:        regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9_-]{8,})?\b`),
		address:    regexp.MustCompile(`(?i)(https?://[^\s"']+|\b(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?\b)`),
		secretFields: map[string]struct{}{
			"prompt": {}, "messages": {}, "completion": {}, "authorization": {}, "api_key": {},
			"token": {}, "capability": {}, "ticket": {}, "idempotency_key": {}, "mover_handle": {},
		},
	}, nil
}

type redactorError struct{ message string }

func (e *redactorError) Error() string { return "observability redactor: " + e.message }

func (r *Redactor) ProviderDiagnostic(body []byte, headers http.Header) string {
	bounded := body
	truncated := false
	if len(bounded) > r.limit {
		bounded = bounded[:r.limit]
		truncated = true
	}
	bounded = bytes.ToValidUTF8(bounded, []byte("?"))
	var decoded any
	if json.Unmarshal(bounded, &decoded) == nil {
		redactJSON(decoded, r.secretFields)
		if sanitized, err := json.Marshal(decoded); err == nil {
			bounded = sanitized
		}
	}
	text := r.credential.ReplaceAllString(string(bounded), "$1=[REDACTED]")
	text = r.jwt.ReplaceAllString(text, "[REDACTED]")
	text = r.address.ReplaceAllString(text, "[ADDRESS]")
	if len(text) > r.limit {
		text = text[:r.limit]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
		truncated = true
	}
	if truncated {
		text += " [truncated]"
	}
	// Header names are useful but values are never copied into diagnostics.
	if len(headers) > 0 {
		names := make([]string, 0, len(headers))
		for name := range headers {
			canonical := strings.ToLower(http.CanonicalHeaderKey(name))
			if canonical == "content-type" || canonical == "retry-after" {
				names = append(names, canonical)
			}
		}
		sort.Strings(names)
		if len(names) > 0 {
			text += " headers=" + strings.Join(names, ",")
		}
	}
	return text
}

func redactJSON(value any, secretFields map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if _, secret := secretFields[strings.ToLower(key)]; secret {
				typed[key] = "[REDACTED]"
				continue
			}
			redactJSON(child, secretFields)
		}
	case []any:
		for _, child := range typed {
			redactJSON(child, secretFields)
		}
	}
}
