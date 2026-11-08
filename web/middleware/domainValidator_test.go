package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestDomainValidator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		domain, host string
		code         int
	}{
		{"panel.example.com", "panel.example.com", http.StatusOK},
		{"panel.example.com", "panel.example.com:54321", http.StatusOK},
		{"Panel.Example.com", "panel.example.com:54321", http.StatusOK},
		{"panel.example.com", "other.example.com:54321", http.StatusForbidden},
		{"192.0.2.1", "192.0.2.1:54321", http.StatusOK},
		{"2001:db8::1", "[2001:db8::1]:54321", http.StatusOK},
		// On the default port an IPv6 host has no port, only brackets.
		{"2001:db8::1", "[2001:db8::1]", http.StatusOK},
		{"2001:db8::1", "[2001:db8::2]", http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.domain+" "+test.host, func(t *testing.T) {
			engine := gin.New()
			engine.Use(DomainValidatorMiddleware(test.domain))
			engine.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Host = test.host
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)
			if w.Code != test.code {
				t.Errorf("got %d, want %d", w.Code, test.code)
			}
		})
	}
}
