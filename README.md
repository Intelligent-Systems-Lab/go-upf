# UPF Event Exposure Service (EES) — MVP

This repository adds a **minimal, standards-aligned Event Exposure Service (EES)** to `go-upf` for the **`USER_DATA_USAGE_MEASURES`** event over **Nupf_EventExposure**. It enables **periodic** and **on-demand** reporting of per-PDU-session usage (UL/DL bytes & packets, derived throughputs), suitable for **UE behavior monitoring and anomaly detection** demos.

> **Design note:** All usage metrics are treated as **interval values over a measurement interval** defined by **`StartTime`** and **`EndTime`** (we do **not** use cumulative counters in the EES path).

---

## Contents

* [What this MVP delivers](#what-this-mvp-delivers)
* [Architecture](#architecture)
* [Added & modified files](#added--modified-files)
* [Configuration](#configuration)
* [Running the demo](#running-the-demo)
* [REST API](#rest-api)
* [Notify payload](#notify-payload)
* [Testing with a simple receiver](#testing-with-a-simple-receiver)
* [Operational notes](#operational-notes)
* [Future work](#future-work)
* [Standards & references](#standards--references)

---

## What this MVP delivers

* **Event:** `USER_DATA_USAGE_MEASURES`
* **Granularity:** per-PDU-session
* **Modes:** `PERIODIC` (every `PeriodSec`) and `ON_DEMAND` (one immediate report using current interval values, then periodic)
* **Target:** `Any UE` (all active sessions)
* **Data:** UL/DL bytes & packets per session over an interval, with optional UL/DL throughputs derived from `(delta bytes * 8) / (EndTime - StartTime)`
* **Transport:** HTTP `POST` from UPF to subscriber’s `notifUri`

---

## Architecture

```
PFCP pipeline (URR / USAReport)
           │
           │  (interval counters per session: UL/DL bytes/packets + StartTime/EndTime)
           ▼
   ees.PFCPSource ─────────► ees.Aggregator (ticks every PeriodSec)
                                      │
                                      │ compute UsageMeasures from interval counters
                                      ▼
                             ees.Notifier  ──►  subscriber notifUri (HTTP POST)
                               ^
                               │
                    ees.SubscriptionStore (in-memory)
```

* **Single source of truth:** We mirror the **existing PFCP USAReport interval data** into EES (no duplicate counters).
* **Interval semantics:** Every record carries **`StartTime`**/`EndTime` for the measurement interval.
* **Simple cleanup:** On each tick, the aggregator removes session snapshots that **do not appear** in the current `SnapshotNow()` result (keeps memory tidy).

---

## Added & modified files

### New (under `internal/ees/`)

* `types.go` – core types (event IDs, granularity, modes, keys, counters, usage measures, subscription, `Source` interface)
* `subscription_store.go` – thread-safe in-memory store for subscriptions (Create/Delete/Get/All)
* `notifier.go` – builds and sends JSON Notify payloads (HTTP POST) with concise, structured logging
* `source_pfcp.go` – implements `Source`; mirrors PFCP USAReport interval data via `OnUSAReport(...)`; `SnapshotNow()` returns a copy of current session counters; includes a simple staleness pruning
* `api.go` – minimal REST server:

  * `POST /nupf-ee/v1/ee-subscriptions` (create)
  * `DELETE /nupf-ee/v1/ee-subscriptions/{id}` (delete)
* `aggregator.go` – periodic/on-demand scheduling, diffing from snapshots, throughput derivation, and per-tick cleanup

### Modified integration points

* `internal/pfcp/report.go` – **mirror** the already-computed per-session **interval** counters (UL/DL bytes, packets, StartTime, EndTime) into `ees.PFCPSource.OnUSAReport(...)` (does **not** change PFCP behavior)
* `pkg/factory/config.go` / `pkg/factory/factory.go` – add `EES` config section and validation
* `cmd/main.go` – wire up `PFCPSource`, `SubscriptionStore`, `Notifier`, `Aggregator`, start EES API server if `EES.Enabled`

---

## Configuration

In `upfcfg.yaml`:

```yaml
EES:
  Enabled: true
  ListenAddr: "0.0.0.0:8088"  # EES HTTP server (create/delete subscriptions)
  PeriodSec: 10               # aggregator tick interval
  LogLevel: "debug"           # inherits global when empty
```

> **Staleness rule:** `PFCPSource` prunes sessions that have not produced interval data for ~`PeriodSec * 3` (configurable in code for MVP). We also remove missing sessions from each subscription’s snapshots after every tick.

---

## Running the demo

1. **Build & run UPF** (ensure your standard `go-upf` prerequisites are met):

```bash
# From the repo root
make
./bin/upf -c ./config/upfcfg.yaml
```

2. **Start a simple receiver** (see below) on the same host (or adjust `notifUri` accordingly):

```bash
python3 receiver.py  # listens on http://127.0.0.1:9000/callback
```

---

## REST API

### Create a subscription

`POST /nupf-ee/v1/ee-subscriptions`

**Request (MVP):**

```json
{
  "notifUri": "http://127.0.0.1:9000/callback",
  "event": "USER_DATA_USAGE_MEASURES",
  "granularity": "perPduSession",
  "mode": "ON_DEMAND",                 // or "PERIODIC"
  "periodSec": 10,
  "target": { "anyUe": true }
}
```

**Response (201):**

```json
{ "subscriptionId": "sub-1730023456789012345-0001" }
```

> **ON_DEMAND**: sends one immediate report using the current interval values, then continues with periodic reporting every `periodSec`.

### Delete a subscription

`DELETE /nupf-ee/v1/ee-subscriptions/{subscriptionId}`

* Returns `204 No Content` for successful deletion.

---

## Notify payload

EES posts the following JSON to each subscription’s `notifUri`:

```json
{
  "subscriptionId": "sub-1730023456789012345-0001",
  "eventId": "USER_DATA_USAGE_MEASURES",
  "granularity": "perPduSession",
  "timestamp": "2025-10-29T13:00:00Z",
  "items": [
    {
      "localSeid":  12345,
      "remoteSeid": 67890,
      "ulBytes":  1048576,
      "dlBytes":  524288,
      "ulPackets": 1200,
      "dlPackets": 800,
      "startTime": "2025-10-29T12:59:50Z",
      "endTime":   "2025-10-29T13:00:00Z",
      "ulThroughputBps": 838860.8,
      "dlThroughputBps": 419430.4
    }
  ]
}
```

* **Interval semantics:** Each item covers exactly the measurement interval `[StartTime, EndTime]`.
* **Throughput fields** are optional and may be omitted when not computed.

---

## Testing with a simple receiver

`receiver.py` (example) listens on `http://127.0.0.1:9000/callback`, prints the full JSON, and replies with `204`:

```bash
python3 receiver.py
```

### Create ON_DEMAND (immediate report, then periodic every 10s)

```bash
curl -sS -X POST http://127.0.0.1:8088/nupf-ee/v1/ee-subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
        "notifUri": "http://127.0.0.1:9000/callback",
        "event": "USER_DATA_USAGE_MEASURES",
        "granularity": "perPduSession",
        "mode": "ON_DEMAND",
        "periodSec": 10,
        "target": { "anyUe": true }
      }'
```

### Create PERIODIC (every 10s)

```bash
curl -sS -X POST http://127.0.0.1:8088/nupf-ee/v1/ee-subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
        "notifUri": "http://127.0.0.1:9000/callback",
        "event": "USER_DATA_USAGE_MEASURES",
        "granularity": "perPduSession",
        "mode": "PERIODIC",
        "periodSec": 10,
        "target": { "anyUe": true }
      }'
```

### Delete

```bash
curl -sS -X DELETE http://127.0.0.1:8088/nupf-ee/v1/ee-subscriptions/SUB_ID
```

---

## Operational notes

* **Interval vs. cumulative:** EES uses **interval** counters mirrored from PFCP; PFCP behavior is unchanged.
* **Cleanup strategy:**

  * `PFCPSource` prunes sessions that have not produced data for a while (approx. `PeriodSec * 3`).
  * After each tick, the aggregator drops session snapshots that are absent in the latest `SnapshotNow()`.
* **Observability:** EES logs include `subscriptionId`, `localSeid`, `remoteSeid`, `startTime`, `endTime`, item counts, and HTTP status codes for Notify results.
* **Safety:** No retries and no authentication in MVP (keep it simple). Add these when integrating with real NWDAF/AF.

---

## Future work

* **Precise on-demand trigger:** `aggregator.NotifyOnceFor(subscriptionId)` to avoid affecting other subscriptions when creating an `ON_DEMAND` one.
* **Dependency injection for PFCPSource:** Replace the temporary global with injected interfaces at PFCP report aggregation points; improves testability and multi-instance hygiene.
* **Configurable staleness:** Expose `staleAfter` in YAML; log the effective `PeriodSec` and `staleAfter` at startup.
* **Targets & filters:** Add UE IP (from PDR) and later DNN/S-NSSAI / application filters; align with Nupf_EventExposure targeting rules.
* **Termination & remaining data:** On N4 session release, send a final report with a termination cause and any remaining data for the last interval.
* **Security & robustness:** Auth (token or mTLS), bounded retries with backoff/jitter, structured error classes, and counters for success/failure.
* **API expansion:** `GET`/`PATCH` for subscription query/update; `/healthz` and `/version` endpoints.
* **NRF Registration for UPF EES**
  UPF EES should register its NF Profile to NRF (NF type = `UPF`) including:

  * Supported service: **Nupf_EventExposure**
  * Supported measurement types (e.g., `USER_DATA_USAGE_MEASURES`)
  * Connectivity information (`nfService`, `ipEndPoints`, `fqdn`)
    This enables other NFs such as **NWDAF** and **DCCF** to discover UPF EES dynamically.

* **NRF-Based NF Discovery for Consumers**
  NWDAF or DCCF, acting as EES consumers, should use **Nnrf_NFDiscovery_Request** to locate UPF instances based on:

  * S-NSSAI
  * DNN
  * DNAI
  * UPF capabilities (e.g., user-data-usage-measurements support)
    This aligns with TS 23.502 §4.15.4.5 (Nnrf-based UPF selection for analytics subscription).

* **Indirect Subscription via SMF with NRF Lookups**
  When NWDAF subscribes indirectly through SMF, SMF should:

  * Query NRF to determine the appropriate UPF for a given PDU session,
  * Then send `Nupf_EventExposure_Subscribe` to that UPF.
    This ensures correct UPF selection even in multi-UPF deployments.

* **UE-IP Direct Subscription (NRF-Assisted Routing)**
  For *Certain UE* targeting (UE IP or SUPI), future EES extensions should:

  * Determine the serving UPF using NRF discovery (TS 23.502 §4.15.4.5.5),
  * Route the direct subscription to the correct UPF instance.

* **NRF Operation Observability**
  Add structured logs and metrics for:

  * NRF registration lifecycle
  * Heartbeats
  * NFDiscovery requests & results
  * UPF service availability updates
    This helps debug distributed deployments and UPF selection behavior.

---

## Standards & references

* **3GPP Nupf_EventExposure** — *User Data Usage Measures* event (per-session/flow/application), periodic and immediate reporting semantics.
* **go-upf PFCP** — URR / USAReport interval measurement as the single source mirrored into EES.
