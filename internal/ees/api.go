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

	"github.com/wmnsk/go-pfcp/ie"
	"go.uber.org/zap"
)

// Server provides the minimal REST API for EES.
type Server struct {
	subscriptionStore *SubscriptionStore
	aggregator        *Aggregator
	provisioner       *Provisioner
	sessionProvider   SessionProvider
	idManager         *IDManager
	logger            *zap.Logger
}

// NewServer constructs a Server.
func NewServer(
	store *SubscriptionStore,
	aggregator *Aggregator,
	provisioner *Provisioner,
	sessionProvider SessionProvider,
	idManager *IDManager,
	logger *zap.Logger,
) *Server {
	return &Server{
		subscriptionStore: store,
		aggregator:        aggregator,
		provisioner:       provisioner,
		sessionProvider:   sessionProvider,
		idManager:         idManager,
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

	// [New] Allocate Shadow URR ID
	if server.idManager != nil {
		shadowID, err := server.idManager.Allocate()
		if err != nil {
			http.Error(w, fmt.Sprintf("allocate shadow id failed: %v", err), http.StatusInternalServerError)
			return
		}
		subscriptionCandidate.ShadowURRID = shadowID
	}

	subscriptionID, err := server.subscriptionStore.CreateSubscription(subscriptionCandidate)
	if err != nil {
		// Rollback ID if create failed
		if server.idManager != nil {
			server.idManager.Release(subscriptionCandidate.ShadowURRID)
		}
		status := http.StatusInternalServerError
		if errors.Is(err, ErrInvalidSubscription) {
			status = http.StatusBadRequest
		}
		http.Error(w, fmt.Sprintf("create subscription failed: %v", err), status)
		return
	}

	// Hybrid Logic: Path A (Immediate/Pull) and Path B (Periodic/Push)

	// Path A: Immediate (ModeOnDemand or logic implies immediate report)
	// If Trigger=ONE_TIME, it's OnDemand.
	if subscriptionCandidate.Mode == ModeOnDemand {
		go func() {
			// Trigger One-Time Pull (using existing polling logic for now, querying standard URRs?)
			// Note: If we want to query Shadow URR, we must create it first.
			// But OnDemand usually means "current status".
			// We use classic Aggregator Tick (Pull) for this.
			if _, tickErr := server.aggregator.TickOnce(context.Background()); tickErr != nil {
				server.logger.Warn("ees on-demand immediate tick failed",
					zap.String("subscriptionId", subscriptionID),
					zap.Error(tickErr),
				)
			}
		}()
	}

	// Path B: Periodic (Push via Kernel Shadow URR)
	if subscriptionCandidate.Mode == ModePeriodic {
		go func() {
			server.provisionURR(subscriptionCandidate)
		}()
	}

	server.logger.Info("ees subscription created",
		zap.String("subscriptionId", subscriptionID),
		zap.Uint32("shadowUrrId", subscriptionCandidate.ShadowURRID), // Log Shadow ID
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

func (server *Server) provisionURR(sub *Subscription) {
	if server.provisioner == nil || server.sessionProvider == nil {
		return
	}

	if sub.Target.AnyUE {
		sessions := server.sessionProvider.GetSessionContexts()
		server.logger.Info("ees provisioning URR to existing sessions",
			zap.Int("count", len(sessions)),
			zap.Uint32("shadowID", sub.ShadowURRID),
		)
		for seid, session := range sessions {
			// 1. Create/Push the EES Shadow URR (Counter)
			err := server.provisioner.PushURRToKernel(
				seid,
				sub.ShadowURRID,
				sub.Event,
				sub.Mode,
				sub.PeriodSec,
			)
			if err != nil {
				server.logger.Warn("ees provisioning failed for session",
					zap.Uint64("seid", seid),
					zap.Error(err),
				)
				continue
			}

			// 2. Bind this new URR to existing PDRs
			// In PFCP, a URR only counts if a PDR points to it.
			// We must update relevant PDRs to include the new URR ID.
			for _, pdr := range session.PDRs {
				// Avoid duplicates
				hasShadow := false
				for _, uid := range pdr.URRIDs {
					if uid == sub.ShadowURRID {
						hasShadow = true
						break
					}
				}
				if hasShadow {
					continue
				}

				// Construct new URR list (Old + Shadow)
				newURRIDs := make([]uint32, len(pdr.URRIDs)+1)
				copy(newURRIDs, pdr.URRIDs)
				newURRIDs[len(pdr.URRIDs)] = sub.ShadowURRID

				// Construct UpdatePDR IE
				// We only send PDR ID and the new list of URR IDs.
				// Based on TS 29.244, other fields remain unchanged if not present.
				ies := []*ie.IE{
					ie.NewPDRID(pdr.PDRID),
				}
				for _, uid := range newURRIDs {
					ies = append(ies, ie.NewURRID(uid))
				}
				updatePdrIE := ie.NewUpdatePDR(ies...)

				// Push to Kernel
				if err := server.provisioner.UpdatePDRToKernel(seid, updatePdrIE); err != nil {
					server.logger.Warn("ees linking PDR to URR failed",
						zap.Uint64("seid", seid),
						zap.Uint16("pdrId", pdr.PDRID),
						zap.Error(err),
					)
				} else {
					// Update local Session Context to reflect the change
					// This ensures future operations know about the link
					pdr.URRIDs = newURRIDs
				}
			}
		}
	}
}

// handleDeleteSubscriptionByID handles DELETE /nupf-ee/v1/ee-subscriptions/{id}
func (server *Server) handleDeleteSubscriptionByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/nupf-ee/v1/ee-subscriptions/")
	if path == "" || strings.Contains(path, "/") {
		http.Error(w, "invalid subscription id in path", http.StatusBadRequest)
		return
	}
	subscriptionID := path

	// Get subscription to find Shadow ID
	sub, found := server.subscriptionStore.GetSubscription(subscriptionID)
	if !found {
		http.Error(w, "subscription not found", http.StatusNotFound)
		return
	}

	// Release ID
	if server.idManager != nil {
		server.idManager.Release(sub.ShadowURRID)
	}

	// Remove URR from Kernel (Best Effort)
	if server.provisioner != nil && server.sessionProvider != nil && sub.Target.AnyUE {
		sessions := server.sessionProvider.GetSessionContexts()
		for seid := range sessions {
			_ = server.provisioner.RemoveURRFromKernel(seid, sub.ShadowURRID)
		}
	}

	if err := server.subscriptionStore.DeleteSubscription(subscriptionID); err != nil {
		// Log error but we already cleaned up resources roughly
		server.logger.Error("delete subscription store failed", zap.Error(err))
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
