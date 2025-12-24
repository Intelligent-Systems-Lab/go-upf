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
)

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

// TargetScope defines the target selection for a subscription.
// MVP: only AnyUE=true (all active PDU sessions).
type TargetScope struct {
	AnyUE bool
	// Future: UEIP, SUPI, DNN, S-NSSAI, app filters...
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
}

// Subscription holds the in-memory state for a single EES subscription.
type Subscription struct {
	ID          string
	NotifURI    string
	Event       EventType
	Target      TargetScope
	Granularity Granularity
	Mode        Mode
	PeriodSec   int

	CreatedAt  time.Time
	LastNotify time.Time

	// Snapshots keeps the last seen per-session counters for delta computation.
	// Key: SessionKey (LocalSEID, RemoteSEID)
	// Val: last counters over [StartTime, EndTime]
	Snapshots map[SessionKey]Counters
}

// Source abstracts the producer of session-level counters for EES.
// SnapshotNow should return a *copy* of current per-session counters, each with
// their StartTime/EndTime representing the measurement interval the counters cover.
type Source interface {
	SnapshotNow() (map[SessionKey]Counters, error)
}

type SessionContext struct {
	RemoteSEID uint64
	URRIDs     []uint32
}
