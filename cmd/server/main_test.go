package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthEndpoint(t *testing.T) {
	r := newRouter()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "\"status\":\"ok\"") {
		t.Fatalf("expected ok response body, got %s", w.Body.String())
	}
}

func TestCreateRoom(t *testing.T) {
	r := newRouter()
	w := httptest.NewRecorder()
	body := strings.NewReader(`{"room_id":"room-1"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/rooms/create", body)
	req.Header.Set("Content-Type", "application/json")

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "room-1") {
		t.Fatalf("expected room in response body, got %s", w.Body.String())
	}
}
