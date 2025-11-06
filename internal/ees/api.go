// Package ees - UPF Event Exposure Service (EES)
// api.go: minimal REST API for EES subscriptions (create/delete).
//
// Endpoints (MVP):
//   POST   /nupf-ee/v1/ee-subscriptions        -> Create subscription
//   DELETE /nupf-ee/v1/ee-subscriptions/{id}   -> Delete subscription
//
// Scope & constraints (MVP):
// - Only USER_DATA_USAGE_MEASURES + perPduSession are accepted.
// - Mode supports PERIODIC and ON_DEMAND; ON_DEMAND triggers an immediate TickOnce().
// - PeriodSec in request is stored in subscription (for future per-subscription period),
//   but the aggregator currently uses a global reportPeriod from config.

package ees

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Server provides the minimal REST API for EES.
type Server struct {
	subscriptionStore *SubscriptionStore
	aggregator        *Aggregator
	logger            *zap.Logger
}

// NewServer constructs a Server.
func NewServer(store *SubscriptionStore, aggregator *Aggregator, logger *zap.Logger) *Server {
	return &Server{
		subscriptionStore: store,
		aggregator:        aggregator,
		logger:            logger,
	}
}

// Routes registers handlers into the given mux.
func (server *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/nupf-ee/v1/ee-subscriptions", server.handleCreateSubscription)      // POST
	mux.HandleFunc("/nupf-ee/v1/ee-subscriptions/", server.handleDeleteSubscriptionByID) // DELETE /.../{id}
}

// Serve starts an HTTP server on the given listen address.
func (server *Server) Serve(listenAddress string) error {
	mux := http.NewServeMux()
	server.Routes(mux)

	server.logger.Info("ees api server listening", zap.String("listenAddr", listenAddress))
	httpServer := &http.Server{
		Addr:              listenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return httpServer.ListenAndServe()
}

// ----- Request/Response models -----

type createSubscriptionRequest struct {
	NotifURI    string `json:"notifUri"`
	Event       string `json:"event"`
	Granularity string `json:"granularity"`
	Mode        string `json:"mode"`
	PeriodSec   int    `json:"periodSec"`

	// MVP: only AnyUE supported; keep the field for future extension.
	Target struct {
		AnyUE bool `json:"anyUe"`
	} `json:"target"`
}

type createSubscriptionResponse struct {
	SubscriptionID string `json:"subscriptionId"`
}

// ----- Handlers -----

// handleCreateSubscription handles POST /nupf-ee/v1/ee-subscriptions
func (server *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var inboundRequest createSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&inboundRequest); err != nil {
		http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
		return
	}

	subscriptionCandidate, validationErr := server.validateAndBuildSubscription(inboundRequest)
	if validationErr != nil {
		http.Error(w, validationErr.Error(), http.StatusBadRequest)
		return
	}

	subscriptionID, err := server.subscriptionStore.CreateSubscription(subscriptionCandidate)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrInvalidSubscription) {
			status = http.StatusBadRequest
		}
		http.Error(w, fmt.Sprintf("create subscription failed: %v", err), status)
		return
	}

	// If ON_DEMAND, trigger one immediate tick (best-effort).
	if subscriptionCandidate.Mode == ModeOnDemand {
		go func() {
			if _, tickErr := server.aggregator.TickOnce(context.Background()); tickErr != nil {
				server.logger.Warn("ees on-demand immediate tick failed",
					zap.String("subscriptionId", subscriptionID),
					zap.Error(tickErr),
				)
			}
		}()
	}

	server.logger.Info("ees subscription created",
		zap.String("subscriptionId", subscriptionID),
		zap.String("notifUri", subscriptionCandidate.NotifURI),
		zap.String("event", string(subscriptionCandidate.Event)),
		zap.String("granularity", string(subscriptionCandidate.Granularity)),
		zap.String("mode", string(subscriptionCandidate.Mode)),
		zap.Int("periodSec", subscriptionCandidate.PeriodSec),
		zap.Bool("targetAnyUE", subscriptionCandidate.Target.AnyUE),
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(createSubscriptionResponse{SubscriptionID: subscriptionID}); err != nil {
		server.logger.Warn("write create-subscription response failed",
			zap.String("subscriptionId", subscriptionID),
			zap.Error(err),
		)
	}
}

// handleDeleteSubscriptionByID handles DELETE /nupf-ee/v1/ee-subscriptions/{id}
func (server *Server) handleDeleteSubscriptionByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Expect path like /nupf-ee/v1/ee-subscriptions/{id}
	path := strings.TrimPrefix(r.URL.Path, "/nupf-ee/v1/ee-subscriptions/")
	if path == "" || strings.Contains(path, "/") {
		http.Error(w, "invalid subscription id in path", http.StatusBadRequest)
		return
	}
	subscriptionID := path

	if err := server.subscriptionStore.DeleteSubscription(subscriptionID); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			http.Error(w, "subscription not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("delete subscription failed: %v", err), http.StatusInternalServerError)
		return
	}

	server.logger.Info("ees subscription deleted", zap.String("subscriptionId", subscriptionID))
	w.WriteHeader(http.StatusNoContent)
}

// ----- Helpers -----

func (server *Server) validateAndBuildSubscription(req createSubscriptionRequest) (*Subscription, error) {
	trimmedNotifURI := strings.TrimSpace(req.NotifURI)
	if trimmedNotifURI == "" {
		return nil, fmt.Errorf("missing field: notifUri")
	}

	// MVP: only USER_DATA_USAGE_MEASURES
	if strings.ToUpper(req.Event) != string(EventUserDataUsageMeasures) {
		return nil, fmt.Errorf("unsupported event: %s (only %s)", req.Event, EventUserDataUsageMeasures)
	}

	// MVP: only perPduSession
	if req.Granularity != string(GranularityPerPduSession) {
		return nil, fmt.Errorf("unsupported granularity: %s (only %s)", req.Granularity, GranularityPerPduSession)
	}

	// Mode validation
	modeUpper := strings.ToUpper(req.Mode)
	var chosenMode Mode
	switch modeUpper {
	case string(ModePeriodic):
		chosenMode = ModePeriodic
	case string(ModeOnDemand):
		chosenMode = ModeOnDemand
	case "":
		// Default to PERIODIC if not specified.
		chosenMode = ModePeriodic
	default:
		return nil, fmt.Errorf("unsupported mode: %s (use %s or %s)", req.Mode, ModePeriodic, ModeOnDemand)
	}

	periodSec := req.PeriodSec
	if periodSec <= 0 {
		// Fallback to aggregator's global period if request omitted or invalid.
		periodSec = int(server.aggregator.reportPeriod / time.Second)
		if periodSec <= 0 {
			periodSec = 10 // ultimate fallback for MVP
		}
	}

	// Build subscription object.
	newSubscription := &Subscription{
		NotifURI:    trimmedNotifURI,
		Event:       EventUserDataUsageMeasures,
		Granularity: GranularityPerPduSession,
		Mode:        chosenMode,
		PeriodSec:   periodSec,
		Target: TargetScope{
			AnyUE: true, // MVP: force AnyUE=true
		},
		Snapshots: make(map[SessionKey]Counters),
	}

	return newSubscription, nil
}
