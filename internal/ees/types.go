// Package ees contains types and interfaces for the UPF Event Exposure Service (EES)
// MVP: USER_DATA_USAGE_MEASURES with per-PDU-session granularity and periodic/on-demand reporting.

package ees

import "time"

// EventType enumerates EES event IDs supported by the UPF.
// MVP only: USER_DATA_USAGE_MEASURES.
type EventType string

const (
	// EventUserDataUsageMeasures reports per-interval usage deltas (bytes/packets, UL/DL),
	// optionally with throughputs derived from (delta bytes) / (EndTime - StartTime).
	EventUserDataUsageMeasures EventType = "USER_DATA_USAGE_MEASURES"
	// EventUserDataUsageTrends reports throughput statistics.
	EventUserDataUsageTrends EventType = "USER_DATA_USAGE_TRENDS"
)

// UserDataUsageMeasurements represents VOLUME-based measurements (Measures event).
type UserDataUsageMeasurements struct {
	VolumeMeasurement VolumeMeasurement `json:"volumeMeasurement"`
	// Time window
	StartTime time.Time `json:"startTime,omitempty"`
	EndTime   time.Time `json:"endTime,omitempty"`
}

type VolumeMeasurement struct {
	TotalVolume     uint64 `json:"totalVolume,omitempty"`
	UplinkVolume    uint64 `json:"uplinkVolume,omitempty"`
	DownlinkVolume  uint64 `json:"downlinkVolume,omitempty"`
	TotalPackets    uint64 `json:"totalPackets,omitempty"`
	UplinkPackets   uint64 `json:"uplinkPackets,omitempty"`
	DownlinkPackets uint64 `json:"downlinkPackets,omitempty"`
}

// ThroughputStatisticMeasurement represents TRENDS-based statistics.
type ThroughputStatisticMeasurement struct {
	UlAverageThroughput float64   `json:"ulAverageThroughput,omitempty"` // bps
	DlAverageThroughput float64   `json:"dlAverageThroughput,omitempty"` // bps
	StartTime           time.Time `json:"startTime,omitempty"`
	EndTime             time.Time `json:"endTime,omitempty"`
}

// Granularity controls the level of aggregation for the event.
// MVP only: perPduSession.
type Granularity string

const (
	GranularityPerPduSession Granularity = "perPduSession"
)

// Mode controls how a subscription is served.
type Mode string

const (
	// ModePeriodic: deliver reports every PeriodSec ticks.
	ModePeriodic Mode = "PERIODIC"
	// ModeOnDemand: deliver one immediate report using the current snapshot
	// relative to the last snapshot (then refresh the snapshot and typically
	// switch to PERIODIC in the aggregator per MVP behavior).
	ModeOnDemand Mode = "ON_DEMAND"
)

// MeasurementType specifies which measurements to include in reports.
// TS 29.564: Required when event type is USER_DATA_USAGE_MEASURES.
type MeasurementType string

const (
	// MeasureVolume requests volume measurements (bytes/packets).
	MeasureVolume MeasurementType = "VOLUME_MEASUREMENT"
	// MeasureThroughput requests throughput measurements (bit rate).
	MeasureThroughput MeasurementType = "THROUGHPUT_MEASUREMENT"
	// MeasureAppInfo requests application-related information.
	MeasureAppInfo MeasurementType = "APPLICATION_RELATED_INFO"
)

// TargetScope defines the target selection for a subscription.
// MVP: only AnyUE=true (all active PDU sessions).
type TargetScope struct {
	AnyUE bool
	// Future: UEIP, SUPI, DNN, S-NSSAI, app filters...
	// UeIPAddress allows targeting a specific UE by IP.
	UeIPAddress string
}

// SessionKey uniquely identifies a session for reporting purposes.
// MVP: use SEIDs; UE IP can be added later without breaking the map key.
type SessionKey struct {
	LocalSEID  uint64 // UPF user-plane SEID
	RemoteSEID uint64 // SMF control-plane SEID
	// Future: UEIP string
}

// Counters represents raw counters collected for a single session over an interval.
// The interval is [StartTime, EndTime]; think "time window start/end" but field names
// are strictly StartTime / EndTime per naming guideline.
type Counters struct {
	ULBytes   uint64
	DLBytes   uint64
	ULPackets uint64
	DLPackets uint64

	StartTime time.Time // measurement interval start time
	EndTime   time.Time // measurement interval end time
}

// UsageMeasures represents a delta (relative to last snapshot) plus derived metrics
// that are ready to be sent in a Notify payload.
type UsageMeasures struct {
	Key SessionKey

	ULBytesDelta   uint64
	DLBytesDelta   uint64
	ULPacketsDelta uint64
	DLPacketsDelta uint64

	StartTime time.Time // interval start
	EndTime   time.Time // interval end

	// Derived metrics (optional in MVP; aggregator may compute them).
	ULThroughputBps float64
	DLThroughputBps float64

	// TS 29.564 UE identifiers
	UeIpv4Addr string // UE IPv4 Address from session context
}

// Subscription holds the in-memory state for a single EES subscription.
type Subscription struct {
	ID                  string
	NotifURI            string
	NotifyCorrelationID string // Added: Client-provided correlation ID
	NfID                string // Added: NF Instance ID
	Event               EventType
	Target              TargetScope
	Granularity         Granularity
	Mode                Mode
	PeriodSec           int

	// TS 29.564: MeasurementTypes specifies which measurements are requested.
	MeasurementTypes []MeasurementType

	CreatedAt  time.Time
	LastNotify time.Time

	// ShadowURRID is the internal URR ID allocated for this subscription.
	ShadowURRID uint32

	// Snapshots keeps the last seen per-session counters for delta computation.
	// Key: SessionKey (LocalSEID, RemoteSEID)
	// Val: last counters over [StartTime, EndTime]
	Snapshots map[SessionKey]Counters
}

// HasMeasurementType checks if the subscription requests the given measurement type.
func (s *Subscription) HasMeasurementType(mt MeasurementType) bool {
	for _, t := range s.MeasurementTypes {
		if t == mt {
			return true
		}
	}
	return false
}

// Source abstracts the producer of session-level counters for EES.
// SnapshotNow should return a *copy* of current per-session counters, each with
// their StartTime/EndTime representing the measurement interval the counters cover.
type Source interface {
	SnapshotNow() (map[SessionKey]Counters, error)
}

type PDRContext struct {
	PDRID  uint16
	URRIDs []uint32
}

type SessionContext struct {
	RemoteSEID uint64
	UeIPv4Addr string // Added: UE IPv4 Address
	URRIDs     []uint32
	PDRs       []*PDRContext
}

// SessionProvider defines the interface for obtaining active Session information.
// Used by API Server for provisioning Shadow URRs to all active sessions.
type SessionProvider interface {
	GetSessionContexts() map[uint64]SessionContext
}
