# UPF Event Exposure Service (EES)

This repository implements the **Nupf_EventExposure** service for `go-upf` per **3GPP TS 29.564**. It enables NFs (SMF, NWDAF, PCF) to subscribe to UPF events such as user data usage measurements.

---

## TS 29.564 Compliance Status

### Supported Features

| Feature | Status | Notes |
|---------|--------|-------|
| **Event Type** | | |
| `USER_DATA_USAGE_MEASURES` | ✅ Supported | Volume + Throughput |
| `USER_DATA_USAGE_TRENDS` | ⚠️ Partial | Schema only |
| `QOS_MONITORING` | ❌ Not Implemented | |
| `TSC_MNGT_INFO` | ❌ Not Implemented | |
| **Measurement Types** | | |
| `VOLUME_MEASUREMENT` | ✅ Supported | Bytes + Packets |
| `THROUGHPUT_MEASUREMENT` | ✅ Supported | Avg bps |
| `APPLICATION_RELATED_INFO` | ⚠️ Schema only | No DPI integration |
| **Granularity** | | |
| `PER_SESSION` | ✅ Supported | Default |
| `PER_APPLICATION` | ⚠️ API only | Requires DPI |
| `PER_FLOW` | ⚠️ API only | Requires DPI |
| **Reporting Trigger** | | |
| `PERIODIC` | ✅ Supported | Configurable period |
| `ONE_TIME` | ✅ Supported | Immediate report |
| **Targeting** | | |
| `anyUe: true` | ✅ Supported | All sessions |
| `ueIpAddress` | ✅ Supported | Specific UE |
| `supi` / `gpsi` | ❌ Not Implemented | |

### Notification Payload (TS 29.564 Compliant)

```json
{
  "notificationItems": [
    {
      "eventType": "USER_DATA_USAGE_MEASURES",
      "timeStamp": "2026-01-14T12:00:00Z",
      "ueIpv4Addr": "10.60.0.1",
      "startTime": "2026-01-14T11:59:30Z",
      "userDataUsageMeasurements": [
        {
          "volumeMeasurement": {
            "totalVolume": 1572864,
            "ulVolume": 524288,
            "dlVolume": 1048576,
            "totalNbOfPackets": 2000,
            "ulNbOfPackets": 800,
            "dlNbOfPackets": 1200
          },
          "throughputStatisticsMeasurement": {
            "ulAverageThroughput": "139810 bps",
            "dlAverageThroughput": "279620 bps"
          }
        }
      ]
    }
  ],
  "correlationId": "corr-12345"
}
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           go-upf                                 │
│  ┌──────────┐    ┌────────────┐    ┌──────────┐                 │
│  │API Server│───▶│ Aggregator │───▶│ Notifier │──▶ HTTP POST    │
│  └────┬─────┘    └─────▲──────┘    └──────────┘                 │
│       │                │ PushReport                              │
│       ▼                │                                         │
│  ┌────────────┐   ┌────┴─────┐                                  │
│  │Provisioner │   │ Handler  │◀── USA Reports                   │
│  └────┬───────┘   └──────────┘                                  │
│       │ CreateURR                                                │
│       ▼                                                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  gtp5g Kernel Module                      │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Configuration

In `upfcfg.yaml`:

```yaml
EES:
  Enabled: true
  ListenAddr: "0.0.0.0:8088"
  PeriodSec: 10
```

---

## REST API

### Create Subscription

`POST /nupf-ee/v1/ee-subscriptions`

### Delete Subscription

`DELETE /nupf-ee/v1/ee-subscriptions/{subscriptionId}`

---

## Example Subscription Payloads

### 1. PER_SESSION (Default)

```bash
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
```

### 2. PER_APPLICATION (Requires appIds and checking only)

```bash
curl -X POST http://127.0.0.1:8088/nupf-ee/v1/ee-subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "subscription": {
      "nfId": "nwdaf-01",
      "eventList": [{
        "type": "USER_DATA_USAGE_MEASURES",
        "measurementTypes": ["VOLUME_MEASUREMENT"],
        "granularityOfMeasurement": "PER_APPLICATION",
        "appIds": ["app-youtube", "app-netflix"]
      }],
      "eventNotifyUri": "http://127.0.0.1:9000/callback",
      "notifyCorrelationId": "corr-app-001",
      "eventReportingMode": {"trigger": "PERIODIC", "reportPeriod": 60},
      "anyUe": true
    }
  }'
```

### 3. PER_FLOW (Requires trafficFilters and checking only)

```bash
curl -X POST http://127.0.0.1:8088/nupf-ee/v1/ee-subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "subscription": {
      "nfId": "pcf-01",
      "eventList": [{
        "type": "USER_DATA_USAGE_MEASURES",
        "measurementTypes": ["VOLUME_MEASUREMENT"],
        "granularityOfMeasurement": "PER_FLOW",
        "trafficFilters": [
          {"flowDescription": "permit in ip from any to 10.0.0.0/8", "flowDirection": "DOWNLINK"}
        ]
      }],
      "eventNotifyUri": "http://127.0.0.1:9000/callback",
      "notifyCorrelationId": "corr-flow-001",
      "eventReportingMode": {"trigger": "PERIODIC", "reportPeriod": 10},
      "ueIpAddress": "10.60.0.1"
    }
  }'
```

### 4. ONE_TIME (Immediate Report but no totoal volume)

```bash
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
      "notifyCorrelationId": "corr-onetime-001",
      "eventReportingMode": {"trigger": "ONE_TIME"},
      "ueIpAddress": "10.60.0.1"
    }
  }'
```

---

## Validation Rules

| Condition | Requirement |
|-----------|-------------|
| `type = USER_DATA_USAGE_MEASURES` | `measurementTypes` is mandatory |
| `granularityOfMeasurement = PER_APPLICATION` | `appIds` is mandatory |
| `granularityOfMeasurement = PER_FLOW` | `trafficFilters` is mandatory |
| Targeting | Either `anyUe: true` OR `ueIpAddress` |

---

## Key Files

| File | Purpose |
|------|---------|
| `internal/ees/api.go` | REST API handlers |
| `internal/ees/aggregator.go` | Report buffering and periodic dispatch |
| `internal/ees/notifier.go` | TS 29.564 payload construction |
| `internal/ees/provisioner.go` | Shadow URR creation |
| `internal/ees/types.go` | Data structures |

---

## Standards References

- **3GPP TS 29.564** — Nupf_EventExposure API
- **3GPP TS 29.244** — PFCP (URR, Usage Reporting)
- **3GPP TS 29.512** — FlowInformation schema
