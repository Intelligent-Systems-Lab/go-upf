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
	NfID                string       `json:"nfId"`
	EventList           []UpfEvent   `json:"eventList"`
	EventNotifyURI      string       `json:"eventNotifyUri"`
	NotifyCorrelationID string       `json:"notifyCorrelationId"`
	EventReportingMode  UpfEventMode `json:"eventReportingMode"`

	// Targeting: choose one
	UeIPAddress string `json:"ueIpAddress,omitempty"`
	AnyUE       bool   `json:"anyUe,omitempty"`
}

type UpfEvent struct {
	Type string `json:"type"`
}

type UpfEventMode struct {
	Trigger      string `json:"trigger"`                // "PERIODIC" | "ONE_TIME"
	ReportPeriod int    `json:"reportPeriod,omitempty"` // Seconds
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
		zap.String("nfId", subscriptionCandidate.NfID),
		zap.String("notifUri", subscriptionCandidate.NotifURI),
		zap.String("event", string(subscriptionCandidate.Event)),
		zap.String("mode", string(subscriptionCandidate.Mode)),
		zap.String("trigger", inboundRequest.EventReportingMode.Trigger),
		zap.Int("periodSec", subscriptionCandidate.PeriodSec),
		zap.Bool("targetAnyUE", subscriptionCandidate.Target.AnyUE),
		zap.String("targetUeIP", subscriptionCandidate.Target.UeIPAddress),
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
	// 1. Check Top-level Mandatory Attributes
	if req.NfID == "" {
		return nil, fmt.Errorf("missing mandatory attribute: nfId")
	}
	if req.EventNotifyURI == "" {
		return nil, fmt.Errorf("missing mandatory attribute: eventNotifyUri")
	}
	if req.NotifyCorrelationID == "" {
		return nil, fmt.Errorf("missing mandatory attribute: notifyCorrelationId")
	}

	// 2. Validate EventList (Mandatory)
	if len(req.EventList) == 0 {
		return nil, fmt.Errorf("missing mandatory attribute: eventList cannot be empty")
	}

	// MVP: Only support exactly one event which matches USER_DATA_USAGE_MEASURES
	foundSupportedEvent := false
	for _, evt := range req.EventList {
		if evt.Type == "" {
			return nil, fmt.Errorf("missing mandatory attribute: eventList[].type")
		}
		if strings.ToUpper(evt.Type) == string(EventUserDataUsageMeasures) {
			foundSupportedEvent = true
		} else {
			// To-do: Support other event types
			return nil, fmt.Errorf("not supported yet: event type %s", evt.Type)
		}
	}
	if !foundSupportedEvent {
		return nil, fmt.Errorf("missing mandatory event type: must include %s", EventUserDataUsageMeasures)
	}

	// 3. Validate EventReportingMode (Mandatory)
	if req.EventReportingMode.Trigger == "" {
		return nil, fmt.Errorf("missing mandatory attribute: eventReportingMode.trigger")
	}

	triggerUpper := strings.ToUpper(req.EventReportingMode.Trigger)
	var chosenMode Mode
	switch triggerUpper {
	case "PERIODIC":
		chosenMode = ModePeriodic
	case "ONE_TIME":
		chosenMode = ModeOnDemand
	default:
		// To-do: Support other triggers (e.g. THRESHOLD)
		return nil, fmt.Errorf("not supported yet: trigger %s", req.EventReportingMode.Trigger)
	}

	periodSec := req.EventReportingMode.ReportPeriod
	if periodSec <= 0 {
		// Fallback for periodic if not specified
		if chosenMode == ModePeriodic {
			periodSec = int(server.aggregator.reportPeriod / time.Second)
			if periodSec <= 0 {
				periodSec = 10
			}
		}
	}

	// 4. Validate Targeting (Conditional Attributes)
	// Must verify one of ueIpAddress or anyUe=true is present.
	target := TargetScope{}
	if req.AnyUE {
		if req.UeIPAddress != "" {
			return nil, fmt.Errorf("invalid targeting: cannot specify both anyUe and ueIpAddress")
		}
		target.AnyUE = true
	} else if req.UeIPAddress != "" {
		// To-do: Implement filtering logic in aggregator to support specific UE targeting
		return nil, fmt.Errorf("not supported yet: targeting specific ueIpAddress")
	} else {
		return nil, fmt.Errorf("missing target: must specify either anyUe=true or provide ueIpAddress")
	}

	// Build subscription object.
	newSubscription := &Subscription{
		NotifURI:            req.EventNotifyURI,
		NotifyCorrelationID: req.NotifyCorrelationID,
		NfID:                req.NfID,
		Event:               EventUserDataUsageMeasures,
		Granularity:         GranularityPerPduSession, // Fixed for MVP
		Mode:                chosenMode,
		PeriodSec:           periodSec,
		Target:              target,
		Snapshots:           make(map[SessionKey]Counters),
	}

	return newSubscription, nil
}

/*
Example Valid Subscription Payload (JSON):

{
  "nfId": "smf-01",
  "eventList": [
    {
      "type": "USER_DATA_USAGE_MEASURES"
    }
  ],
  "eventNotifyUri": "http://10.0.0.10:8080/namf-callback/v1/nupf-event",
  "notifyCorrelationId": "corr-12345",
  "eventReportingMode": {
    "trigger": "PERIODIC",
    "reportPeriod": 10
  },
  "anyUe": true
}
*/
