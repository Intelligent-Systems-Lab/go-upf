// Package ees - UPF Event Exposure Service (EES)
// source_pfcp.go: PFCP-backed Source implementation for EES (interval semantics, multi-URR aware).
//
// - Store per-session measurements per-URR: entriesBySessionKey[SessionKey][URRID] = pfcpEntry.
// - OnUSAReport(sessionKey, urrID, intervalCounters) writes the latest interval for that URR.
// - SnapshotNow() keeps the original Source interface:
//     It MERGES all FRESH (not stale) URR entries under the same SessionKey into one Counters:
//       * UL/DL bytes/packets: summed over URR
//       * StartTime: min over URR
//       * EndTime:   max over URR
//   A session is included only if it has at least one fresh URR entry.
// - Staleness: an URR entry is fresh if (now - lastUpdateTime) <= staleAfter.

package ees

import (
	"fmt"
	"sync"
	"time"

	"github.com/free5gc/go-upf/internal/report"
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

// ForwarderDriver 定義與底層轉發層 (如 gtp5g) 溝通的介面
// 對應 internal/forwarder/driver.go 中的實作
type ForwarderDriver interface {
	QueryMultiURR(map[uint64][]uint32) (map[uint64][]report.USAReport, error)
}

// SessionProvider 定義獲取活躍 Session 資訊的介面
// 對應 internal/pfcp/node.go 中 LocalNode 的實作
type SessionProvider interface {
	GetSessionContexts() map[uint64]SessionContext
}

// [實作 Source]
// ActivePFCPSource 是一個無狀態的 Source，每次 SnapshotNow 都會主動向底層查詢最新數據。
type ActivePFCPSource struct {
	driver          ForwarderDriver
	sessionProvider SessionProvider
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

// NewActivePFCPSource 建構子
func NewActivePFCPSource(driver ForwarderDriver, provider SessionProvider) *ActivePFCPSource {
	return &ActivePFCPSource{
		driver:          driver,
		sessionProvider: provider,
	}
}

// SnapshotNow 執行主動查詢 (Active Pull)
// 1. 從 Provider 獲取所有 Session 上下文
// 2. 向 Driver 批量查詢 URR 數據
// 3. 聚合數據並回傳
func (s *ActivePFCPSource) SnapshotNow() (map[SessionKey]Counters, error) {
	// 1. 獲取當前活躍的 Session 列表
	sessionCtxs := s.sessionProvider.GetSessionContexts()
	if len(sessionCtxs) == 0 {
		return nil, nil
	}

	// 2. 準備批量查詢的參數 (map[LocalSEID] -> []URRID)
	queryMap := make(map[uint64][]uint32, len(sessionCtxs))
	for lSeid, ctx := range sessionCtxs {
		if len(ctx.URRIDs) > 0 {
			queryMap[lSeid] = ctx.URRIDs
		}
	}

	if len(queryMap) == 0 {
		return nil, nil
	}

	// 3. 呼叫 Driver 執行批量查詢 (Netlink 交互)
	reportsMap, err := s.driver.QueryMultiURR(queryMap)

	for _, reports := range reportsMap {
		for _, r := range reports {
			// 強制印出所有 URR 的資訊，不管是不是 0
			fmt.Printf("[DEBUG-EES] URR:%d UL:%d DL:%d\n",
				r.URRID, r.VolumMeasure.UplinkVolume, r.VolumMeasure.DownlinkVolume)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("active query failed: %w", err)
	}

	// 4. 轉換並聚合結果
	result := make(map[SessionKey]Counters, len(reportsMap))

	for lSeid, reports := range reportsMap {
		ctx, ok := sessionCtxs[lSeid]
		if !ok {
			// 在查詢期間 Session 可能剛好被刪除，忽略此結果
			continue
		}

		// 聚合該 Session 下多個 URR 的數據
		var merged Counters
		var hasFresh bool

		for _, r := range reports {
			// 如果需要過濾 Stale 數據 (例如 Kernel 很久沒更新)，可以在這裡判斷 r.EndTime
			// 但通常 Active Query 取得的都是 Kernel 當下的數值

			if !hasFresh {
				// 第一筆數據，直接初始化
				merged = Counters{
					ULBytes:   r.VolumMeasure.UplinkVolume,
					DLBytes:   r.VolumMeasure.DownlinkVolume,
					ULPackets: r.VolumMeasure.UplinkPktNum,
					DLPackets: r.VolumMeasure.DownlinkPktNum,
					StartTime: r.StartTime,
					EndTime:   r.EndTime,
				}
				hasFresh = true
			} else {
				// 後續數據，進行累加
				merged.ULBytes += r.VolumMeasure.UplinkVolume
				merged.DLBytes += r.VolumMeasure.DownlinkVolume
				merged.ULPackets += r.VolumMeasure.UplinkPktNum
				merged.DLPackets += r.VolumMeasure.DownlinkPktNum

				// 時間區間取聯集 (Start 取最早，End 取最晚)
				if !r.StartTime.IsZero() && r.StartTime.Before(merged.StartTime) {
					merged.StartTime = r.StartTime
				}
				if !r.EndTime.IsZero() && r.EndTime.After(merged.EndTime) {
					merged.EndTime = r.EndTime
				}
			}
		}

		if hasFresh {
			key := SessionKey{
				LocalSEID:  lSeid,
				RemoteSEID: ctx.RemoteSEID,
			}
			result[key] = merged
		}
	}

	return result, nil
}
