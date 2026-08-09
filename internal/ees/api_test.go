package ees

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestCreateSubscriptionUsesNupfEventExposureContract(t *testing.T) {
	store := NewSubscriptionStore("test")
	server := NewServer(
		store,
		&Aggregator{reportPeriod: 30 * time.Second},
		logrus.NewEntry(logrus.New()),
		nil,
	)
	mux := http.NewServeMux()
	server.Routes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	requestBody := `{
		"subscription": {
			"eventList": [{
				"type": "USER_DATA_USAGE_MEASURES",
				"measurementTypes": ["VOLUME_MEASUREMENT"],
				"granularityOfMeasurement": "PER_SESSION"
			}],
			"eventNotifyUri": "http://127.0.0.1:9091/callbacks/upf-event-exposure",
			"notifyCorrelationId": "corr-1",
			"eventReportingMode": {
				"trigger": "PERIODIC",
				"repPeriod": 10
			},
			"nfId": "smf-instance",
			"ueIpAddress": {
				"ipv4Addr": "10.10.0.1"
			}
		}
	}`
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		httpServer.URL+"/nupf-ee/v1/ee-subscriptions",
		strings.NewReader(requestBody),
	)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("close response body: %v", closeErr)
		}
	}()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want %d", response.StatusCode, http.StatusCreated)
	}

	var body createSubscriptionResponse
	if err = json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Subscription.EventReportingMode.RepPeriod != 10 {
		t.Fatalf(
			"response repPeriod: got %d, want 10",
			body.Subscription.EventReportingMode.RepPeriod,
		)
	}
	if body.Subscription.UEIPAddress == nil ||
		body.Subscription.UEIPAddress.IPv4Addr != "10.10.0.1" {
		t.Fatalf("response ueIpAddress: got %+v", body.Subscription.UEIPAddress)
	}
	if body.SubscriptionID == "" {
		t.Fatal("response subscriptionId is empty")
	}
	stored, found := store.GetSubscription(body.SubscriptionID)
	if !found {
		t.Fatal("created subscription is missing from the store")
	}
	if stored.PeriodSec != 10 || stored.Target.UeIPAddress != "10.10.0.1" {
		t.Fatalf("stored subscription does not preserve the request: %+v", stored)
	}
}
