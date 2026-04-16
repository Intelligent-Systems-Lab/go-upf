// Package ees - UPF Event Exposure Service (EES)
// pseudodriver.go: reads historical traffic data from a Parquet file and replays
// it as EES notifications (warm start) when a subscription is created.
//
// Behavior:
// - On subscription creation, LoadAndReplay() is called in a goroutine.
// - Reads per-packet records from the Parquet file.
// - Aggregates packets into time windows matching the subscription's reportPeriod.
// - Sends each time window as an EES Notify (same format as live data).
// - After all historical data is sent, live data continues via the normal pipeline.

package ees

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

// PseudoDriver reads Parquet files of historical per-packet traffic data
// and replays them as EES notifications to provide a warm start for subscribers.
type PseudoDriver struct {
	parquetDir string
	notifier   *Notifier
	logger     *zap.Logger

	aggregator *Aggregator

	// FirstURRSignal is signaled by the Aggregator when the first URR report arrives.
	FirstURRSignal chan time.Time
	firstURROnce   sync.Once

	// Phase 2 state: tracks the current simulation window so that
	// Kernel reports can be aligned to the same time grid.
	isSimulating    bool
	phase2StartTime time.Time
	phase2EndTime   time.Time
	simMu           sync.RWMutex
}

// NewPseudoDriver constructs a PseudoDriver.
func NewPseudoDriver(parquetDir string, notifier *Notifier, logger *zap.Logger) *PseudoDriver {
	return &PseudoDriver{
		parquetDir:     parquetDir,
		notifier:       notifier,
		logger:         logger,
		FirstURRSignal: make(chan time.Time, 1),
	}
}

// SetAggregator injects the Aggregator reference.
func (pd *PseudoDriver) SetAggregator(agg interface{}) {
	// We use interface{} to avoid circular dependency if they are in different packages,
	// but they are both in `ees` package, so we can use *Aggregator directly.
	pd.aggregator = agg.(*Aggregator)
}

// SignalFirstURR is called by the Aggregator when the first URR report arrives.
// It sends the URR's StartTime to the pseudo driver so it can anchor its timeline.
// This is safe to call multiple times; only the first call has effect.
func (pd *PseudoDriver) SignalFirstURR(urrStartTime time.Time) {
	if pd == nil {
		return
	}
	pd.firstURROnce.Do(func() {
		pd.FirstURRSignal <- urrStartTime
		pd.logger.Info("pseudo driver: first URR signal sent",
			zap.Time("urrStartTime", urrStartTime),
		)
	})
}

// GetPhase2Window returns the current Phase 2 window's start/end times.
// If Phase 2 is not active, the bool return is false.
// Used by PushReport to align Kernel reports to Phase 2's logical timeline.
func (pd *PseudoDriver) GetPhase2Window() (startTime, endTime time.Time, active bool) {
	if pd == nil {
		return time.Time{}, time.Time{}, false
	}
	pd.simMu.RLock()
	defer pd.simMu.RUnlock()
	return pd.phase2StartTime, pd.phase2EndTime, pd.isSimulating
}

// ParquetRow represents a single row from the historical traffic Parquet file.
// The raw Parquet file contains 24 columns, including many string flags and IPs with null values:
// ts, direction, len, action, ue_ip, session_file, ue_id, flow_id, adjusted_timestamp,
// source_dataset, app_name, category, activity, session_id, session_duration,
// relative_time, pkt_len, l4_proto, src_ip, dst_ip, src_port, dst_port, tcp_flags, iat.
//
// We intentionally map only the 5 critical fields (ts, direction, len, action, ue_ip)
// to drastically reduce memory usage, improve garbage collection during streaming,
// and natively bypass Parquet-Go parsing warnings triggered by Null strings in unused columns.
type ParquetRow struct {
	Timestamp float64 `parquet:"ts"`
	Direction string  `parquet:"direction"`
	Len       int64   `parquet:"len"`
	Action    string  `parquet:"action"`
	UeIP      string  `parquet:"ue_ip"`
}

// parsedPacket holds the parsed fields needed for aggregation.
type parsedPacket struct {
	ueIP      string
	timestamp float64 // seconds from epoch / start
	pktLen    uint64
	isUplink  bool // direction "0" = UL, "1" = DL
}

// windowKey groups packets for aggregation.
type windowKey struct {
	ueIP        string
	windowIndex int
}

// counterAccum holds accumulated counters for a single (ueIP, windowIndex) pair.
type counterAccum struct {
	ulBytes   uint64
	dlBytes   uint64
	ulPkts    uint64
	dlPkts    uint64
	startTime float64
	endTime   float64
}

// accumulatePacket adds a parsed packet's data to the corresponding counter in the map.
func accumulatePacket(accum map[windowKey]*counterAccum, key windowKey, pkt parsedPacket) {
	ca, ok := accum[key]
	if !ok {
		ca = &counterAccum{
			startTime: pkt.timestamp,
			endTime:   pkt.timestamp,
		}
		accum[key] = ca
	}

	if pkt.isUplink {
		ca.ulBytes += pkt.pktLen
		ca.ulPkts++
	} else {
		ca.dlBytes += pkt.pktLen
		ca.dlPkts++
	}

	if pkt.timestamp < ca.startTime {
		ca.startTime = pkt.timestamp
	}
	if pkt.timestamp > ca.endTime {
		ca.endTime = pkt.timestamp
	}
}

// LoadAndReplay reads the Parquet files from directory chronologically,
// aggregates packets into corresponding continuous time windows,
// and sends them as EES notifications to the given subscription.
// This method is designed to be called in a goroutine.
func (pd *PseudoDriver) LoadAndReplay(sub *Subscription) {
	if pd == nil || pd.parquetDir == "" {
		return
	}

	pd.logger.Info("pseudo driver starting warm-start replay stream",
		zap.String("subscriptionId", sub.ID),
		zap.String("parquetDir", pd.parquetDir),
		zap.Int("periodSec", sub.PeriodSec),
	)

	// Read file.json for breaking time
	metaPath := pd.parquetDir + "/file.json"
	metaBytes, err := os.ReadFile(metaPath)
	breakingTimeSec := 0.0
	if err == nil {
		var meta struct {
			BreakingTime float64 `json:"breaking time"`
		}
		if err := json.Unmarshal(metaBytes, &meta); err == nil {
			breakingTimeSec = meta.BreakingTime
		}
	}
	if breakingTimeSec <= 0 {
		breakingTimeSec = 300 // default
	}

	pd.logger.Info("pseudo driver: using breaking time", zap.Float64("breakingTimeSec", breakingTimeSec))

	// 1. Scan directory for Parquet files
	entries, err := os.ReadDir(pd.parquetDir)
	if err != nil {
		pd.logger.Error("pseudo driver failed to read parquet directory",
			zap.String("subscriptionId", sub.ID),
			zap.Error(err),
		)
		return
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > 8 && entry.Name()[len(entry.Name())-8:] == ".parquet" {
			files = append(files, pd.parquetDir+"/"+entry.Name())
		}
	}

	if len(files) == 0 {
		pd.logger.Warn("pseudo driver: no parquet files found in directory",
			zap.String("subscriptionId", sub.ID),
			zap.String("directory", pd.parquetDir),
		)
		return
	}

	sort.Strings(files)

	periodSec := sub.PeriodSec
	if periodSec <= 0 {
		periodSec = 5 // default fallback
	}
	period := float64(periodSec)

	// CRITICAL GRID ALIGNMENT:
	// We align breakingTimeSec to the next multiple of periodSec.
	alignedBreakingTime := math.Ceil(breakingTimeSec/period) * period

	// =====================================================================
	// STREAMING AGGREGATION (Memory-efficient)
	// Instead of storing all parsed packets in memory, we read each file
	// and directly accumulate into counterAccum maps. This reduces peak
	// memory from O(total_packets) to O(unique_UEs × unique_windows).
	// =====================================================================
	pd.logger.Info("pseudo driver: streaming parquet files into aggregation maps")
	phase1Accum := make(map[windowKey]*counterAccum)
	phase2Accum := make(map[windowKey]*counterAccum)
	uniqueUEs := make(map[string]bool)
	globalTimeOffset := 0.0
	var phase1PktCount, phase2PktCount int

	for _, fileObj := range files {
		// Pass 1: Scan for min/max timestamps without storing any rows
		minTS, maxTS, rowCount, scanErr := pd.scanTimestampRange(fileObj, sub)
		if scanErr != nil || rowCount == 0 {
			continue
		}

		// Pass 2: Re-read file, process each row directly into accum maps
		p1, p2 := pd.streamAndAccumulate(
			fileObj, sub, minTS, globalTimeOffset,
			alignedBreakingTime, period,
			phase1Accum, phase2Accum, uniqueUEs,
		)
		phase1PktCount += p1
		phase2PktCount += p2
		globalTimeOffset += (maxTS - minTS) + 0.001
	}

	pd.logger.Info("pseudo driver: streaming aggregation complete",
		zap.Int("phase1Entries", len(phase1Accum)),
		zap.Int("phase2Entries", len(phase2Accum)),
		zap.Int("phase1Packets", phase1PktCount),
		zap.Int("phase2Packets", phase2PktCount),
		zap.Int("uniqueUEs", len(uniqueUEs)),
	)

	// Wait for the first URR report from the kernel, then anchor to the
	// NEXT TickOnce trigger time. This aligns the entire time grid to the
	// Aggregator's heartbeat, eliminating sub-second drift.
	var anchorTime time.Time
	const urrWaitTimeout = 60 * time.Second
	pd.logger.Info("pseudo driver: waiting for first URR signal to anchor timeline",
		zap.Duration("timeout", urrWaitTimeout),
	)

	select {
	case urrTime := <-pd.FirstURRSignal:
		pd.logger.Info("pseudo driver: first URR received, anchoring timeline instantly")
		// FIX: Do NOT wait for TickOnce to complete. If we wait, we miss the current tick
		// and fall 5 seconds (one period) behind the Kernel's real-time reporting.
		// By anchoring immediately using the exact time Kernel pushed its first report,
		// we guarantee Phase 1 and Phase 2's first window enter the buffer BEFORE the
		// Aggregator's TickOnce fires.
		anchorTime = urrTime.Round(time.Second) // Align to clean second boundary
		pd.logger.Info("pseudo driver: anchored to Kernel URR time",
			zap.Time("anchorTime", anchorTime),
		)
	case <-time.After(urrWaitTimeout):
		anchorTime = time.Now()
		pd.logger.Warn("pseudo driver: URR wait timed out, falling back to time.Now()",
			zap.Duration("timeout", urrWaitTimeout),
		)
	}

	// Absolute reference time for Parquet offset 0
	// alignedBreakingTime corresponds to the Anchor.
	// So offset 0 corresponds to Anchor - alignedBreakingTime
	referenceTime := anchorTime.Add(-time.Duration(alignedBreakingTime * float64(time.Second)))

	// CRITICAL: Set the subscription's GridAnchor to our referenceTime.
	// This is the SINGLE SOURCE OF TRUTH for the entire time grid.
	sub.GridAnchor = referenceTime
	pd.logger.Info("pseudo driver: grid alignment established",
		zap.Time("gridAnchor", referenceTime),
		zap.Time("anchorTime", anchorTime),
		zap.Float64("alignedBreakingTime", alignedBreakingTime),
	)

	// =====================================================================
	// 5. Build Phase 2 windows from pre-aggregated accum map.
	// =====================================================================
	var phase2Windows [][]UsageMeasures
	if len(phase2Accum) > 0 {
		endP2WIdx := 0
		for key := range phase2Accum {
			if key.windowIndex > endP2WIdx {
				endP2WIdx = key.windowIndex
			}
		}
		phase2Windows = pd.buildWindowsFromAccum(phase2Accum, uniqueUEs, periodSec, referenceTime, endP2WIdx)
		phase2Accum = nil // Allow GC to reclaim
	}

	// =====================================================================
	// PHASE 1: Warmstart (Historical Burst - Near Instant)
	// =====================================================================
	pd.logger.Info("pseudo driver: executing Phase 1 (Warmstart)",
		zap.Int("packets", phase1PktCount),
	)

	// Pre-activate simulation state so Kernel reports align to GridAnchor immediately.
	pd.simMu.Lock()
	pd.isSimulating = true
	// Establish the first window of Phase 2 for Kernel snapping
	startWIdx := int(alignedBreakingTime / period)
	firstP2Start := referenceTime.Add(time.Duration(float64(startWIdx)*period) * time.Second)
	pd.phase2StartTime = firstP2Start
	pd.phase2EndTime = firstP2Start.Add(time.Duration(periodSec) * time.Second)
	pd.simMu.Unlock()

	if len(phase1Accum) > 0 {
		endWIdx := int(alignedBreakingTime/period) - 1
		windows := pd.buildWindowsFromAccum(phase1Accum, uniqueUEs, periodSec, referenceTime, endWIdx)
		phase1Accum = nil // Allow GC to reclaim
		for wIdx, measures := range windows {
			logicalTime := referenceTime.Add(time.Duration((wIdx+1)*periodSec) * time.Second)
			if pd.aggregator != nil {
				pd.aggregator.PushHistoricalMeasures(sub, measures, logicalTime)
			}
			// Instant burst - No sleep.
		}
	} else {
		// Fast-forward LastNotify
		sub.LastNotify = anchorTime
	}

	// =====================================================================
	// DEBUG: Phase 1 → Phase 2 transition state dump
	// =====================================================================
	pd.logger.Info("pseudo driver: === PHASE TRANSITION 1→2 STATE DUMP ===",
		zap.Time("anchorTime", anchorTime),
		zap.Time("gridAnchor", sub.GridAnchor),
		zap.Time("lastNotify", sub.LastNotify),
		zap.Float64("alignedBreakingTimeSec", alignedBreakingTime),
		zap.Int("phase1PacketCount", phase1PktCount),
		zap.Int("phase2PacketCount", phase2PktCount),
		zap.Int("phase2WindowCount", len(phase2Windows)),
		zap.Int("startWIdx", startWIdx),
		zap.Bool("isSimulating", pd.isSimulating),
		zap.Time("phase2StartTime", pd.phase2StartTime),
		zap.Time("phase2EndTime", pd.phase2EndTime),
		zap.Time("wallClockNow", time.Now()),
	)

	// =====================================================================
	// PHASE 2: Parallel Future Simulation (Synchronized pacing)
	// =====================================================================
	if len(phase2Windows) > 0 && pd.aggregator != nil {
		pd.simulateFutureRealTime(sub, phase2Windows, startWIdx)
	}
}

// matchesSubscriptionFilter checks if a UE IP matches the subscription's target scope.
func matchesSubscriptionFilter(sub *Subscription, ueIP string) bool {
	if sub.Target.AnyUE {
		return true
	}
	return sub.Target.UeIPAddress != "" && sub.Target.UeIPAddress == ueIP
}

// scanTimestampRange reads through a Parquet file to find the min/max timestamps
// of rows matching the subscription filter. No row data is stored in memory.
func (pd *PseudoDriver) scanTimestampRange(filePath string, sub *Subscription) (minTS, maxTS float64, rowCount int, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open parquet file: %w", err)
	}
	defer f.Close()

	reader := parquet.NewReader(f)
	defer reader.Close()

	first := true
	for {
		var row ParquetRow
		if readErr := reader.Read(&row); readErr != nil {
			break
		}
		if row.UeIP == "" {
			continue
		}
		if !matchesSubscriptionFilter(sub, row.UeIP) {
			continue
		}

		if first {
			minTS = row.Timestamp
			maxTS = row.Timestamp
			first = false
		} else {
			if row.Timestamp < minTS {
				minTS = row.Timestamp
			}
			if row.Timestamp > maxTS {
				maxTS = row.Timestamp
			}
		}
		rowCount++
	}
	return
}

// streamAndAccumulate re-reads a Parquet file and accumulates each matching row
// directly into the phase1/phase2 accum maps. No intermediate slice is created.
func (pd *PseudoDriver) streamAndAccumulate(
	filePath string, sub *Subscription,
	minTS, globalTimeOffset, alignedBreakingTime, period float64,
	phase1Accum, phase2Accum map[windowKey]*counterAccum,
	uniqueUEs map[string]bool,
) (phase1Count, phase2Count int) {
	f, err := os.Open(filePath)
	if err != nil {
		pd.logger.Error("pseudo driver: failed to re-open parquet file",
			zap.String("file", filePath), zap.Error(err))
		return
	}
	defer f.Close()

	reader := parquet.NewReader(f)
	defer reader.Close()

	for {
		var row ParquetRow
		if readErr := reader.Read(&row); readErr != nil {
			break
		}
		if row.UeIP == "" {
			continue
		}
		if !matchesSubscriptionFilter(sub, row.UeIP) {
			continue
		}

		globalTS := (row.Timestamp - minTS) + globalTimeOffset
		wIdx := int(math.Floor(globalTS / period))
		key := windowKey{ueIP: row.UeIP, windowIndex: wIdx}
		uniqueUEs[row.UeIP] = true

		pkt := parsedPacket{
			ueIP:      row.UeIP,
			timestamp: globalTS,
			pktLen:    uint64(row.Len),
			isUplink:  row.Direction == "0",
		}

		if globalTS <= alignedBreakingTime {
			accumulatePacket(phase1Accum, key, pkt)
			phase1Count++
		} else {
			accumulatePacket(phase2Accum, key, pkt)
			phase2Count++
		}
	}
	return
}

// buildWindowsFromAccum converts a pre-built counterAccum map into ordered [][]UsageMeasures.
// This replaces aggregateIntoWindows: instead of iterating over raw packets to build
// the accum map internally, it accepts an already-built map from streaming aggregation.
func (pd *PseudoDriver) buildWindowsFromAccum(
	accum map[windowKey]*counterAccum,
	uniqueUEs map[string]bool,
	periodSec int,
	referenceTime time.Time,
	endWindowIndex int,
) [][]UsageMeasures {
	if len(accum) == 0 {
		return nil
	}

	period := float64(periodSec)

	// Determine actual max window index
	maxWindowIndex := endWindowIndex
	for key := range accum {
		if key.windowIndex > maxWindowIndex {
			maxWindowIndex = key.windowIndex
		}
	}

	// Build output: one []UsageMeasures per window (may contain multiple UEs)
	windowResults := make(map[int][]UsageMeasures)

	for wIdx := 0; wIdx <= maxWindowIndex; wIdx++ {
		windowStartOff := float64(wIdx) * period
		windowEndOff := windowStartOff + period

		offsetStart := time.Duration(math.Round(windowStartOff * float64(time.Second)))
		offsetEnd := time.Duration(math.Round(windowEndOff * float64(time.Second)))

		defaultStartT := referenceTime.Add(offsetStart).Truncate(time.Millisecond)
		defaultEndT := referenceTime.Add(offsetEnd).Truncate(time.Millisecond)

		for ueIP := range uniqueUEs {
			key := windowKey{ueIP: ueIP, windowIndex: wIdx}
			ca, exists := accum[key]

			var m UsageMeasures
			if exists {
				m = UsageMeasures{
					Key:            SessionKey{LocalSEID: uint64(wIdx + 1)},
					ULBytesDelta:   ca.ulBytes,
					DLBytesDelta:   ca.dlBytes,
					ULPacketsDelta: ca.ulPkts,
					DLPacketsDelta: ca.dlPkts,
					StartTime:      defaultStartT,
					EndTime:        defaultEndT,
					UeIpv4Addr:     ueIP,
				}
			} else {
				m = UsageMeasures{
					Key:            SessionKey{LocalSEID: uint64(wIdx + 1)},
					StartTime:      defaultStartT,
					EndTime:        defaultEndT,
					UeIpv4Addr:     ueIP,
				}
			}
			computeThroughputIfPossible(&m)
			windowResults[wIdx] = append(windowResults[wIdx], m)
		}
	}

	// Sort by window index and return
	sortedIndices := make([]int, 0, len(windowResults))
	for idx := range windowResults {
		sortedIndices = append(sortedIndices, idx)
	}
	sort.Ints(sortedIndices)

	result := make([][]UsageMeasures, 0, len(sortedIndices))
	for _, idx := range sortedIndices {
		result = append(result, windowResults[idx])
	}

	return result
}

func (pd *PseudoDriver) simulateFutureRealTime(sub *Subscription, windows [][]UsageMeasures, startWIdx int) {
	defer func() {
		pd.simMu.Lock()
		pd.isSimulating = false
		pd.simMu.Unlock()
		pd.logger.Info("pseudo driver: Phase 2 simulation ended, Kernel reports back to normal")
	}()

	pd.logger.Info("pseudo driver: Phase 2 future simulation pacing loop started",
		zap.Int("windowsToPace", len(windows)-startWIdx),
	)

	// Clean up any stale signals accumulated during history replay.
	pd.aggregator.DrainTickDone()

	// DEBUG: Dump Aggregator buffer state at Phase 2 entry
	pd.aggregator.DebugDumpBufferState("Phase2-Entry")

	// STAGE 2: AGGREGATOR-SYNCHRONIZED PACING.
	// We iterate through pre-computed windows starting from the first live index.
	//
	// CRITICAL FIX: The loop order is "Wait → Push" (not "Push → Wait").
	// Rationale:
	//   After TickOnce[N-1] fires, BOTH Phase 2 and Kernel push data for window N
	//   into the buffer over the next ~5 seconds. Then TickOnce[N] fires and
	//   collects them together in one consolidated notification.
	//
	//   If we used "Push → Wait", Phase 2 would push window N+1's empty padding
	//   immediately after TickOnce[N], but Kernel's real data for N+1 wouldn't
	//   arrive until the NEXT Kernel timer fires (~5s later). So Phase 2's empty
	//   padding would go out in TickOnce[N+1] while Kernel's real data would only
	//   appear in TickOnce[N+2] — permanently out of sync by one period.
	for i := startWIdx; i < len(windows); i++ {
		winMeasures := windows[i]

		// 1. Wait for TickOnce[N-1] to finish first.
		//    This marks the beginning of the ~5s interval leading up to TickOnce[N].
		waitStart := time.Now()
		pd.aggregator.WaitForTick()
		waitDuration := time.Since(waitStart)

		// 2. Publish current Phase 2 window time for Kernel report alignment.
		//    Kernel reports arriving during the next ~5s wait will use THIS window.
		if len(winMeasures) > 0 {
			pd.simMu.Lock()
			oldP2Start := pd.phase2StartTime
			oldP2End := pd.phase2EndTime
			pd.phase2StartTime = winMeasures[0].StartTime
			pd.phase2EndTime = winMeasures[0].EndTime
			pd.simMu.Unlock()

			pd.logger.Info("pseudo driver: Phase 2 window published",
				zap.Int("windowIndex", i),
				zap.Time("newP2Start", winMeasures[0].StartTime),
				zap.Time("newP2End", winMeasures[0].EndTime),
				zap.Time("prevP2Start", oldP2Start),
				zap.Time("prevP2End", oldP2End),
				zap.Time("wallClockNow", time.Now()),
			)
		}

		pd.logger.Info("pseudo driver: Phase 2 WaitForTick returned",
			zap.Int("windowIndex", i),
			zap.Duration("waitDuration", waitDuration),
			zap.Time("wallClockNow", time.Now()),
			zap.Time("aggLastTickTime", pd.aggregator.GetLastTickTime()),
		)

		// 3. NOW push this window's data into Buffer.
		//    Kernel's data for this same window will arrive over the next ~5s.
		//    Both will be collected together by TickOnce[N].
		if len(winMeasures) > 0 && pd.aggregator != nil {
			pd.logger.Info("pseudo driver: pushing Phase 2 data",
				zap.Int("windowIndex", i),
				zap.Int("measures", len(winMeasures)),
				zap.Time("dataStartTime", winMeasures[0].StartTime),
				zap.Time("dataEndTime", winMeasures[0].EndTime),
				zap.Time("wallClockNow", time.Now()),
			)
			pd.aggregator.PushLiveMeasures(sub, winMeasures)
		}
	}

	pd.logger.Info("pseudo driver: Phase 2 future simulation completed")
}
