# Comparison Report: go-upf vs go-upf-ees

This report details the differences between the UPF implementation in `go-upf` and the active workspace `go-upf-ees`.

## Summary of Changes
The primary difference is the integration of the **Event Exposure Service (EES)** into `go-upf-ees`. This includes a new `ees` package, configuration updates, and hooks within the PFCP and Forwarder layers to forward traffic usage reports and session events to the EES module.

## 1. New Components & Files
The following files and directories exist only in `go-upf-ees`:

### Core EES Implementation
*   **`internal/ees/`**: A new package containing the core logic for the Event Exposure Service, including:
    *   Subscription management (Store).
    *   Notification handling (Notifier).
    *   Data aggregation (Aggregator).
    *   API Server implementation.

### Application Layer Extensions
*   **`pkg/app/dispatcher.go`**: A new dispatcher component to handle report routing, acting as an intermediary between the driver's report handling and the EES. Also modify the reporting period of aggregator 

### Configuration & Tools
*   **`ees.yaml`**: The configuration file for EES.
*   **`UPF-EES.yaml`**: A UPF configuration file tailored for EES scenarios.
*   **`receiver.py`**: A helper script for testing EES notifications.

## 2. Modified Files
The following files have been modified to support EES integration:

### Configuration
*   **`pkg/factory/config.go`**
    *   Added `EES *EESConfig` field to the `Config` struct.
    *   Defined `EESConfig` struct with fields: `Enabled`, `ListenAddr`, `PeriodSec`, and `LogLevel`.

### Application Logic (`pkg/app/app.go`)
*   **Enhanced `Run()` method**:
    *   Initialization of `reportDispatcher`.
    *   **EES Module Startup**: Added logic to initialize and start the EES module in "Pure Push Mode" if enabled in config. Which means no other volume collecting method other than copying URR periodical reports is used
    *   Setup of EES components: `SubscriptionStore`, `Notifier`, `Aggregator`, and `Server`.
    *   Wiring of `onURRAdded` callback from `perio.Server` to trigger EES aggregator periodadjustments.

### Packet Forwarding Control Protocol (PFCP)
*   **`internal/pfcp/node.go`**
    *   **Session Context**: Added `UeIPv4Addr` to the `Sess` struct to track UE IP addresses.
    *   **UE IP Extraction**: Added logic to parse `UEIPAddress` IE from PDI and populate `UeIPv4Addr`.
    *   **Session Provider Interface**: Implemented `GetSessionContexts()` method on `LocalNode` to expose active session data (RemoteSEID, URRIDs, PDRs) to the EES module.
*   **`internal/pfcp/pfcp.go`**
    *   Added `GetLocalNode()` accessor to `PfcpServer` to allow EES access to the local node instance.
*   **`internal/pfcp/report.go`**
    *   Added logging for the number of Usage Report (USAR) items.
    *   Added `URSEQN` (Usage Report Sequence Number) assignment logic (`r.URSEQN = sess.URRSeq(r.URRID)`).

### Forwarder / Driver Layer
*   **`internal/forwarder/gtp5g.go`**
    *   Added `GetPerioServer()`: Exposes `perio.Server` instance.
    *   Fixed/Added `MeasurementMethod` parsing in `newFlowDesc`.
*   **`internal/forwarder/perio/server.go`**
    *   Added `onURRAdded` callback mechanism to notify external components (EES) when a new URR period is registered.
    *   Added `SetOnURRAdded` setter.
    *   Added `GetAnyURRPeriod` helper method to query URR periods across sessions.

### Build & Dependencies
*   **`go.mod` / `go.sum`**: Updated dependencies, likely adding `zap` (logging) or internal utility packages required by the new EES code.

## 3. Detailed Config Changes
The `Config` struct in `pkg/factory/config.go` now supports an optional `ees` block:

```yaml
ees:
  enabled: true
  listenAddr: ":8088"
  periodSec: 10
  logLevel: "info"
```

## Conclusion
The `go-upf` codebase is a superexpansion of the original `free5gc` UPF, explicitly modified to support the **3GPP Event Exposure Service (EES)**. It adds a completely new architectural layer (`internal/ees`) and intrusively modifies the existing PFCP and Forwarding components to extract the necessary data (Session contexts, URR usage, UE IPs) required for EES reporting.
