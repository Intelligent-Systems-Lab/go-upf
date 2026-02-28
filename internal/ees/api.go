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
// - Uses SMF-provisioned URRs for data collection (no Shadow URR).

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
func NewServer(
	store *SubscriptionStore,
	aggregator *Aggregator,
	logger *zap.Logger,
) *Server {
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
	Subscription UpfEventSubscription `json:"subscription"`
	// SupportedFeatures string `json:"supportedFeatures,omitempty"` // Not implemented yet
}

type UpfEventSubscription struct {
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
	Type             string   `json:"type"`
	MeasurementTypes []string `json:"measurementTypes,omitempty"`
	// TS 29.564: Required when type=USER_DATA_USAGE_MEASURES
	GranularityOfMeasurement string `json:"granularityOfMeasurement,omitempty"`
	// PER_SESSION, PER_APPLICATION, PER_FLOW
	AppIds []string `json:"appIds,omitempty"`
	// Required for PER_APPLICATION
	TrafficFilters []FlowInformation `json:"trafficFilters,omitempty"`
	// Required for PER_FLOW
}

type UpfEventMode struct {
	Trigger      string `json:"trigger"`                // "PERIODIC" | "ONE_TIME"
	ReportPeriod int    `json:"reportPeriod,omitempty"` // Seconds
}

type createSubscriptionResponse struct {
	Subscription   UpfEventSubscription `json:"subscription"`
	SubscriptionID string               `json:"subscriptionId"`
	// ReportList     []NotificationItem   `json:"reportList,omitempty"` // Not implemented in immediate response yet
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

	// On-demand mode: trigger immediate report
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
		zap.String("trigger", inboundRequest.Subscription.EventReportingMode.Trigger),
		zap.Int("periodSec", subscriptionCandidate.PeriodSec),
		zap.Bool("targetAnyUE", subscriptionCandidate.Target.AnyUE),
		zap.String("targetUeIP", subscriptionCandidate.Target.UeIPAddress),
	)

	locationURI := fmt.Sprintf("%s/nupf-ee/v1/ee-subscriptions/%s", getAPIRoot(r), subscriptionID)
	w.Header().Set("Location", locationURI)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	// Build full subscription response per TS 29.564
	response := createSubscriptionResponse{
		SubscriptionID: subscriptionID,
		Subscription: UpfEventSubscription{
			NfID:                subscriptionCandidate.NfID,
			EventList:           inboundRequest.Subscription.EventList,
			EventNotifyURI:      subscriptionCandidate.NotifURI,
			NotifyCorrelationID: subscriptionCandidate.NotifyCorrelationID,
			EventReportingMode: UpfEventMode{
				Trigger:      inboundRequest.Subscription.EventReportingMode.Trigger,
				ReportPeriod: subscriptionCandidate.PeriodSec,
			},
			AnyUE:       subscriptionCandidate.Target.AnyUE,
			UeIPAddress: subscriptionCandidate.Target.UeIPAddress,
		},
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
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

	path := strings.TrimPrefix(r.URL.Path, "/nupf-ee/v1/ee-subscriptions/")
	if path == "" || strings.Contains(path, "/") {
		http.Error(w, "invalid subscription id in path", http.StatusBadRequest)
		return
	}
	subscriptionID := path

	// Check subscription exists
	_, found := server.subscriptionStore.GetSubscription(subscriptionID)
	if !found {
		http.Error(w, "subscription not found", http.StatusNotFound)
		return
	}

	if err := server.subscriptionStore.DeleteSubscription(subscriptionID); err != nil {
		server.logger.Error("delete subscription store failed", zap.Error(err))
	}

	server.logger.Info("ees subscription deleted", zap.String("subscriptionId", subscriptionID))
	w.WriteHeader(http.StatusNoContent)
}

// ----- Helpers -----

func (server *Server) validateAndBuildSubscription(req createSubscriptionRequest) (*Subscription, error) {
	sub := req.Subscription
	// 1. Check Top-level Mandatory Attributes
	if sub.NfID == "" {
		return nil, fmt.Errorf("missing mandatory attribute: nfId")
	}
	if sub.EventNotifyURI == "" {
		return nil, fmt.Errorf("missing mandatory attribute: eventNotifyUri")
	}
	if sub.NotifyCorrelationID == "" {
		return nil, fmt.Errorf("missing mandatory attribute: notifyCorrelationId")
	}

	// 2. Validate EventList (Mandatory)
	if len(sub.EventList) == 0 {
		return nil, fmt.Errorf("missing mandatory attribute: eventList cannot be empty")
	}

	// MVP: Only support exactly one event which matches USER_DATA_USAGE_MEASURES
	foundSupportedEvent := false
	var measurementTypes []MeasurementType
	granularity := GranularityPerSession // Default
	var appIds []string
	var trafficFilters []FlowInformation

	for _, evt := range sub.EventList {
		if evt.Type == "" {
			return nil, fmt.Errorf("missing mandatory attribute: eventList[].type")
		}
		if strings.ToUpper(evt.Type) == string(EventUserDataUsageMeasures) {
			foundSupportedEvent = true

			// TS 29.564: measurementTypes is mandatory for USER_DATA_USAGE_MEASURES
			if len(evt.MeasurementTypes) == 0 {
				return nil, fmt.Errorf("missing mandatory attribute: measurementTypes is required for USER_DATA_USAGE_MEASURES")
			}

			// Convert string types to MeasurementType
			for _, mt := range evt.MeasurementTypes {
				switch strings.ToUpper(mt) {
				case string(MeasureVolume):
					measurementTypes = append(measurementTypes, MeasureVolume)
				case string(MeasureThroughput):
					measurementTypes = append(measurementTypes, MeasureThroughput)
				case string(MeasureAppInfo):
					measurementTypes = append(measurementTypes, MeasureAppInfo)
				default:
					return nil, fmt.Errorf("unsupported measurementType: %s", mt)
				}
			}

			// Parse granularityOfMeasurement (optional, defaults to PER_SESSION)
			if evt.GranularityOfMeasurement != "" {
				switch strings.ToUpper(evt.GranularityOfMeasurement) {
				case string(GranularityPerSession):
					granularity = GranularityPerSession
				case string(GranularityPerApplication):
					granularity = GranularityPerApplication
					// Validate: PER_APPLICATION requires appIds
					if len(evt.AppIds) == 0 {
						return nil, fmt.Errorf(
							"missing mandatory attribute: appIds required for PER_APPLICATION")
					}
					appIds = evt.AppIds
				case string(GranularityPerFlow):
					granularity = GranularityPerFlow
					// Validate: PER_FLOW requires trafficFilters
					if len(evt.TrafficFilters) == 0 {
						return nil, fmt.Errorf(
							"missing mandatory attribute: trafficFilters required for PER_FLOW")
					}
					trafficFilters = evt.TrafficFilters
				default:
					return nil, fmt.Errorf("unsupported granularityOfMeasurement: %s", evt.GranularityOfMeasurement)
				}
			}
		} else {
			// To-do: Support other event types
			return nil, fmt.Errorf("not supported yet: event type %s", evt.Type)
		}
	}
	if !foundSupportedEvent {
		return nil, fmt.Errorf("missing mandatory event type: must include %s", EventUserDataUsageMeasures)
	}

	// 3. Validate EventReportingMode (Mandatory)
	if sub.EventReportingMode.Trigger == "" {
		return nil, fmt.Errorf("missing mandatory attribute: eventReportingMode.trigger")
	}

	triggerUpper := strings.ToUpper(sub.EventReportingMode.Trigger)
	var chosenMode Mode
	switch triggerUpper {
	case "PERIODIC":
		chosenMode = ModePeriodic
	case "ONE_TIME":
		chosenMode = ModeOnDemand
	default:
		// To-do: Support other triggers (e.g. THRESHOLD)
		return nil, fmt.Errorf("not supported yet: trigger %s", sub.EventReportingMode.Trigger)
	}

	periodSec := sub.EventReportingMode.ReportPeriod
	if periodSec <= 0 {
		// Fallback for periodic if not specified
		if chosenMode == ModePeriodic {
			periodSec = int(server.aggregator.reportPeriod / time.Second)
			if periodSec <= 0 {
				periodSec = 10
			}
		}
	}

	// Validate period against URR measurement period (if available)
	if chosenMode == ModePeriodic && server.aggregator.perioServer != nil {
		// Try to get URR 2 period from perio.Server
		urrPeriod := server.aggregator.perioServer.GetAnyURRPeriod(2)
		if urrPeriod > 0 {
			urrPeriodSec := int(urrPeriod.Seconds())

			// Validation 1: Subscription period must not be shorter than URR period
			if periodSec < urrPeriodSec {
				return nil, fmt.Errorf(
					"invalid reporting period: requested %ds is shorter than URR measurement period %ds",
					periodSec, urrPeriodSec,
				)
			}

			// Validation 2: Subscription period must be a multiple of URR period
			if periodSec%urrPeriodSec != 0 {
				return nil, fmt.Errorf(
					"invalid reporting period: requested %ds is not a multiple of URR measurement period %ds",
					periodSec, urrPeriodSec,
				)
			}

			server.logger.Debug("ees subscription period validated",
				zap.Int("requestedPeriod", periodSec),
				zap.Int("urrPeriod", urrPeriodSec),
			)
		} else {
			// URR 2 not yet established, log warning but allow subscription
			server.logger.Warn("ees cannot validate period: URR 2 not found in perio server",
				zap.Int("requestedPeriod", periodSec),
			)
		}
	}

	// 4. Validate Targeting (Conditional Attributes)
	// Must verify one of ueIpAddress or anyUe=true is present.
	target := TargetScope{}
	if sub.AnyUE {
		if sub.UeIPAddress != "" {
			return nil, fmt.Errorf("invalid targeting: cannot specify both anyUe and ueIpAddress")
		}
		target.AnyUE = true
	} else if sub.UeIPAddress != "" {
		target.UeIPAddress = sub.UeIPAddress
	} else {
		return nil, fmt.Errorf("missing target: must specify either anyUe=true or provide ueIpAddress")
	}

	// Build subscription object.
	newSubscription := &Subscription{
		NotifURI:            sub.EventNotifyURI,
		NotifyCorrelationID: sub.NotifyCorrelationID,
		NfID:                sub.NfID,
		Event:               EventUserDataUsageMeasures,
		Granularity:         granularity,
		Mode:                chosenMode,
		PeriodSec:           periodSec,
		Target:              target,
		MeasurementTypes:    measurementTypes,
		AppIds:              appIds,
		TrafficFilters:      trafficFilters,
		Snapshots:           make(map[SessionKey]Counters),
	}

	return newSubscription, nil
}

// getAPIRoot helper to construct the base URL from the request.
// In a real deployment, this might be configured, but using the request Host is a good default.
func getAPIRoot(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	// If behind a proxy, X-Forwarded-Proto might be needed, but keeping it simple for MVP.
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}

/*
Example Subscription Payloads (JSON):

=== 1. PER_SESSION (Default) ===
curl -X POST http://127.0.0.1:8088/nupf-ee/v1/ee-subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "subscription": {
      "nfId": "smf-01",
      "eventList": [{
        "type": "USER_DATA_USAGE_MEASURES",
        "measurementTypes": ["VOLUME_MEASUREMENT", "THROUGHPUT_MEASUREMENT"],
        "granularityOfMeasurement": "PER_SESSION"
      }],
      "eventNotifyUri": "http://127.0.0.1:9000/callback",
      "notifyCorrelationId": "corr-session-001",
      "eventReportingMode": {"trigger": "PERIODIC", "reportPeriod": 30},
      "anyUe": true
    }
  }'

=== 2. Specific UE Targeting ===
curl -X POST http://127.0.0.1:8088/nupf-ee/v1/ee-subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "subscription": {
      "nfId": "smf-01",
      "eventList": [{
        "type": "USER_DATA_USAGE_MEASURES",
        "measurementTypes": ["VOLUME_MEASUREMENT"]
      }],
      "eventNotifyUri": "http://127.0.0.1:9000/callback",
      "notifyCorrelationId": "corr-ue-001",
      "eventReportingMode": {"trigger": "ONE_TIME"},
      "ueIpAddress": "10.10.0.1"
    }
  }'
*/
