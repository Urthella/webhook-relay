package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

const maxRequestBody = 1 << 20

// API is the HTTP interface applications use to send webhooks.
type API struct {
	Store        *Store
	Token        string
	AllowPrivate bool
	Log          *slog.Logger
}

// Handler routes requests. Everything except /healthz needs the bearer token.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/endpoints", a.createEndpoint)
	mux.HandleFunc("POST /v1/endpoints/{id}/enable", a.enableEndpoint)
	mux.HandleFunc("POST /v1/messages", a.createMessage)
	mux.HandleFunc("GET /v1/messages/{id}", a.getMessage)
	mux.HandleFunc("POST /v1/messages/{id}/redrive", a.redrive)
	auth := func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+a.Token)) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or wrong bearer token")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
		mux.ServeHTTP(w, r)
	}
	outer := http.NewServeMux()
	outer.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	outer.HandleFunc("/", auth)
	return outer
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// serverError logs the details and tells the client only that it failed.
func (a *API) serverError(w http.ResponseWriter, err error) {
	a.Log.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func (a *API) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := checkEndpointURL(req.URL, a.AllowPrivate); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	e, err := a.Store.createEndpoint(r.Context(), req.URL)
	if err != nil {
		a.serverError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, e)
}

func (a *API) enableEndpoint(w http.ResponseWriter, r *http.Request) {
	found, err := a.Store.enableEndpoint(r.Context(), r.PathValue("id"))
	switch {
	case err != nil:
		a.serverError(w, err)
	case !found:
		writeError(w, http.StatusNotFound, "no such endpoint")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *API) createMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EndpointID     string          `json:"endpoint_id"`
		Type           string          `json:"type"`
		Data           json.RawMessage `json:"data"`
		IdempotencyKey *string         `json:"idempotency_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON, or a body over 1 MB")
		return
	}
	if req.EndpointID == "" || req.Type == "" || len(req.Data) == 0 {
		writeError(w, http.StatusBadRequest, "endpoint_id, type and data are required")
		return
	}
	id, err := a.Store.enqueue(r.Context(), req.EndpointID, req.Type, req.Data, req.IdempotencyKey)
	switch {
	case errors.Is(err, ErrUnknownEndpoint):
		writeError(w, http.StatusNotFound, "no such endpoint")
	case err != nil:
		a.serverError(w, err)
	default:
		writeJSON(w, http.StatusAccepted, map[string]string{"id": id})
	}
}

func (a *API) getMessage(w http.ResponseWriter, r *http.Request) {
	m, err := a.Store.message(r.Context(), r.PathValue("id"))
	switch {
	case err != nil:
		a.serverError(w, err)
	case m == nil:
		writeError(w, http.StatusNotFound, "no such message")
	default:
		writeJSON(w, http.StatusOK, m)
	}
}

func (a *API) redrive(w http.ResponseWriter, r *http.Request) {
	found, err := a.Store.redrive(r.Context(), r.PathValue("id"))
	switch {
	case err != nil:
		a.serverError(w, err)
	case !found:
		writeError(w, http.StatusConflict, "no dead message with that id")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
