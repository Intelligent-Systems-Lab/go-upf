// Package ees - UPF Event Exposure Service (EES)
// source_pfcp.go: PFCP-backed Source implementation for EES.
//
// DEPRECATED: This file contains legacy PFCPSource for passive report caching.
// The current EES uses pure Push mode via aggregator.PushReport().
// PFCPSource is kept for potential future use.

package ees

import (
	"sync"
	"time"
)

// pfcpEntry stores one URR's latest interval counters and its last update time.
type pfcpEntry struct {
	latestCounters Counters
	lastUpdateTime time.Time
}

// PFCPSource implements Source for EES, backed by PFCP usage reports (URR/USAReport).
// It aggregates multiple URRs per session at SnapshotNow() time.
type PFCPSource struct {
	mutexForEntries        sync.RWMutex
	entriesBySessionAndURR map[SessionKey]map[uint32]pfcpEntry

	// staleAfter controls staleness per URR entry. If now - lastUpdateTime > staleAfter,
	// that URR entry is considered stale (excluded from merge).
	staleAfter time.Duration
}

// NewPFCPSource creates a PFCPSource with the given staleness threshold.
// A practical default is around 3×reporting period (e.g., 30s for a 10s period).
func NewPFCPSource(staleAfter time.Duration) *PFCPSource {
	if staleAfter <= 0 {
		staleAfter = 30 * time.Second
	}
	return &PFCPSource{
		entriesBySessionAndURR: make(map[SessionKey]map[uint32]pfcpEntry),
		staleAfter:             staleAfter,
	}
}

// SetStaleAfter changes the staleness threshold at runtime (optional).
func (pfcpSource *PFCPSource) SetStaleAfter(newStaleAfter time.Duration) {
	if newStaleAfter <= 0 {
		return
	}
	pfcpSource.mutexForEntries.Lock()
	pfcpSource.staleAfter = newStaleAfter
	pfcpSource.mutexForEntries.Unlock()
}

// OnUSAReport should be called by the PFCP reporting pipeline per URR interval.
// intervalCounters MUST represent an interval [StartTime, EndTime] for that URR.
//
// Note: This implements Plan A (multi-URR). We DO NOT overwrite other URRs;
// we store by URRID, and merge later at SnapshotNow().
func (pfcpSource *PFCPSource) OnUSAReport(
	sessionKey SessionKey,
	urrID uint32,
	intervalCounters Counters,
) {
	now := time.Now()

	pfcpSource.mutexForEntries.Lock()
	defer pfcpSource.mutexForEntries.Unlock()

	urrMap, ok := pfcpSource.entriesBySessionAndURR[sessionKey]
	if !ok {
		urrMap = make(map[uint32]pfcpEntry)
		pfcpSource.entriesBySessionAndURR[sessionKey] = urrMap
	}
	urrMap[urrID] = pfcpEntry{
		latestCounters: intervalCounters,
		lastUpdateTime: now,
	}
}

// SnapshotNow merges all FRESH URR entries under each session into a single Counters.
// Rules:
//   - Fresh URR: (now - lastUpdateTime) <= staleAfter
//   - UL/DL bytes/packets: sum over fresh URRs
//   - StartTime: min over fresh URRs
//   - EndTime:   max over fresh URRs
//
// A session is included only if at least one URR is fresh.

//this is the old version kept for reference
/*
func (pfcpSource *PFCPSource) SnapshotNow() (map[SessionKey]Counters, error) {
	now := time.Now()

	pfcpSource.mutexForEntries.RLock()
	defer pfcpSource.mutexForEntries.RUnlock()

	mergedBySession := make(map[SessionKey]Counters, len(pfcpSource.entriesBySessionAndURR))

	for sessionKey, urrMap := range pfcpSource.entriesBySessionAndURR {
		var merged Counters
		var hasFresh bool

		for urrID, urrEntry := range urrMap {
			_ = urrID // reserved for future per-URR debug/log

			if now.Sub(urrEntry.lastUpdateTime) > pfcpSource.staleAfter {
				continue // stale URR entry; skip
			}
			urrIntervalCounters := urrEntry.latestCounters

			if !hasFresh {
				// initialize from the first fresh URR
				merged = Counters{
					ULBytes:   urrIntervalCounters.ULBytes,
					DLBytes:   urrIntervalCounters.DLBytes,
					ULPackets: urrIntervalCounters.ULPackets,
					DLPackets: urrIntervalCounters.DLPackets,
					StartTime: urrIntervalCounters.StartTime,
					EndTime:   urrIntervalCounters.EndTime,
				}
				hasFresh = true
				continue
			} else {
				// sum bytes/packets across URRs
				merged.ULBytes += urrIntervalCounters.ULBytes
				merged.DLBytes += urrIntervalCounters.DLBytes
				merged.ULPackets += urrIntervalCounters.ULPackets
				merged.DLPackets += urrIntervalCounters.DLPackets
			}
			// expand interval boundaries
			if urrIntervalCounters.StartTime.Before(merged.StartTime) {
				merged.StartTime = urrIntervalCounters.StartTime
			}
			if urrIntervalCounters.EndTime.After(merged.EndTime) {
				merged.EndTime = urrIntervalCounters.EndTime
			}
		}

		if hasFresh {
			mergedBySession[sessionKey] = merged
		}
	}

	return mergedBySession, nil
}*/
//end of old version

// PruneStale removes sessions that have no fresh URR entries (all URRs are stale).
// This is optional; the aggregator already cleans per-subscription snapshots.
// Keeping this helps PFCPSource memory stay compact over long runs.
func (pfcpSource *PFCPSource) PruneStale() (removedCount int) {
	now := time.Now()

	pfcpSource.mutexForEntries.Lock()
	defer pfcpSource.mutexForEntries.Unlock()

	for sessionKey, urrMap := range pfcpSource.entriesBySessionAndURR {
		hasFresh := false
		for _, entry := range urrMap {
			if now.Sub(entry.lastUpdateTime) <= pfcpSource.staleAfter {
				hasFresh = true
				break
			}
		}
		if !hasFresh {
			delete(pfcpSource.entriesBySessionAndURR, sessionKey)
			removedCount++
		}
	}
	return removedCount
}
