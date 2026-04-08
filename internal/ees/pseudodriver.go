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
	// PRE-READ ALL PARQUET FILES (SLOW IO OPERATION)
	// We MUST do this BEFORE waiting for URR/anchorTime to avoid missing
	// the Aggregator's Ticker heartbeat during the multi-second load time.
	// =====================================================================
	pd.logger.Info("pseudo driver: pre-reading and filtering parquet files")
	var phase1Packets []parsedPacket
	var phase2Packets []parsedPacket
	globalTimeOffset := 0.0

	for _, fileObj := range files {
		packets, readErr := pd.readParquetFile(fileObj)
		if readErr != nil || len(packets) == 0 {
			continue
		}
		filtered := pd.filterBySubscription(packets, sub)
		if len(filtered) == 0 {
			continue
		}

		minTS := filtered[0].timestamp
		maxTS := filtered[0].timestamp
		for _, pkt := range filtered[1:] {
			if pkt.timestamp < minTS {
				minTS = pkt.timestamp
			}
			if pkt.timestamp > maxTS {
				maxTS = pkt.timestamp
			}
		}

		// Adjust timestamps to unified global timeline
		for i := range filtered {
			globalTS := (filtered[i].timestamp - minTS) + globalTimeOffset
			filtered[i].timestamp = globalTS
			if globalTS <= alignedBreakingTime {
				phase1Packets = append(phase1Packets, filtered[i])
			} else {
				phase2Packets = append(phase2Packets, filtered[i])
			}
		}
		globalTimeOffset += (maxTS - minTS) + 0.001
	}

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
	// 5. [NEW] PRE-COMPUTE all Phase 2 window data upfront.
	// This ensures the transition from History to Live is instant.
	// =====================================================================
	var phase2Windows [][]UsageMeasures
	if len(phase2Packets) > 0 {
		maxP2TS := 0.0
		for _, pkt := range phase2Packets {
			if pkt.timestamp > maxP2TS {
				maxP2TS = pkt.timestamp
			}
		}
		endP2WIdx := int(math.Floor(maxP2TS / period))
		phase2Windows = pd.aggregateIntoWindows(phase2Packets, periodSec, referenceTime, endP2WIdx)
	}

	// =====================================================================
	// PHASE 1: Warmstart (Historical Burst - Near Instant)
	// =====================================================================
	pd.logger.Info("pseudo driver: executing Phase 1 (Warmstart)",
		zap.Int("packets", len(phase1Packets)),
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

	if len(phase1Packets) > 0 {
		endWIdx := int(alignedBreakingTime/period) - 1
		windows := pd.aggregateIntoWindows(phase1Packets, periodSec, referenceTime, endWIdx)
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
		zap.Int("phase1PacketCount", len(phase1Packets)),
		zap.Int("phase2PacketCount", len(phase2Packets)),
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

// readParquetFile reads all rows from the specified Parquet file and converts to parsedPacket.
func (pd *PseudoDriver) readParquetFile(filePath string) ([]parsedPacket, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}
	defer f.Close()

	reader := parquet.NewReader(f)
	defer reader.Close()

	var packets []parsedPacket
	parseErrors := 0

	for {
		var row ParquetRow
		err := reader.Read(&row)
		if err != nil {
			break // EOF or error
		}

		pkt, parseErr := parseRow(row)
		if parseErr != nil {
			parseErrors++
			continue
		}
		packets = append(packets, pkt)
	}

	if parseErrors > 0 {
		pd.logger.Warn("pseudo driver: some rows failed to parse",
			zap.String("file", filePath),
			zap.Int("parseErrors", parseErrors),
			zap.Int("successfulRows", len(packets)),
		)
	}

	return packets, nil
}

// parseRow converts a ParquetRow to a parsedPacket.
func parseRow(row ParquetRow) (parsedPacket, error) {
	if row.UeIP == "" {
		return parsedPacket{}, fmt.Errorf("skip empty row")
	}

	isUplink := row.Direction == "0"

	return parsedPacket{
		ueIP:      row.UeIP,
		timestamp: row.Timestamp,
		pktLen:    uint64(row.Len),
		isUplink:  isUplink,
	}, nil
}

// filterBySubscription filters packets based on the subscription's target scope.
func (pd *PseudoDriver) filterBySubscription(packets []parsedPacket, sub *Subscription) []parsedPacket {
	if sub.Target.AnyUE {
		return packets // no filtering needed
	}

	if sub.Target.UeIPAddress == "" {
		return nil
	}

	filtered := make([]parsedPacket, 0, len(packets)/2)
	for _, pkt := range packets {
		if pkt.ueIP == sub.Target.UeIPAddress {
			filtered = append(filtered, pkt)
		}
	}
	return filtered
}

// aggregateIntoWindows groups packets into time windows and produces
// a slice of []UsageMeasures for each window, ordered by time.
// Since timestamps are already shifted, window indices naturally align globally.
func (pd *PseudoDriver) aggregateIntoWindows(packets []parsedPacket, periodSec int, referenceTime time.Time, endWindowIndex int) [][]UsageMeasures {
	if len(packets) == 0 {
		return nil
	}

	period := float64(periodSec)

	// Build aggregation map: windowKey -> accumulated counters
	type counterAccum struct {
		ulBytes   uint64
		dlBytes   uint64
		ulPkts    uint64
		dlPkts    uint64
		startTime float64
		endTime   float64
	}

	accum := make(map[windowKey]*counterAccum)
	uniqueUEs := make(map[string]bool)
	maxWindowIndex := endWindowIndex

	for _, pkt := range packets {
		// global offset is 0-indexed across all files now
		wIdx := int(math.Floor(pkt.timestamp / period))
		if wIdx > maxWindowIndex {
			maxWindowIndex = wIdx
		}
		uniqueUEs[pkt.ueIP] = true

		key := windowKey{ueIP: pkt.ueIP, windowIndex: wIdx}

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

	// Build output: one []UsageMeasures per window (may contain multiple UEs)
	// For every window from 0 to maxWindowIndex, and every unique UE, we output a report.
	windowResults := make(map[int][]UsageMeasures)

	for wIdx := 0; wIdx <= maxWindowIndex; wIdx++ {
		windowStartOff := float64(wIdx) * period
		windowEndOff := windowStartOff + period

		// Use integer conversion and truncate to Millisecond to completely eliminate
		// float64 precision noise. This guarantees the generated time will perfectly
		// match the snapTime() logic in the Aggregator.
		offsetStart := time.Duration(math.Round(windowStartOff * float64(time.Second)))
		offsetEnd := time.Duration(math.Round(windowEndOff * float64(time.Second)))

		defaultStartT := referenceTime.Add(offsetStart).Truncate(time.Millisecond)
		defaultEndT := referenceTime.Add(offsetEnd).Truncate(time.Millisecond)

		for ueIP := range uniqueUEs {
			key := windowKey{ueIP: ueIP, windowIndex: wIdx}
			ca, exists := accum[key]

			var m UsageMeasures
			if exists {
				// Align StartTime/EndTime to fixed window boundaries so that
				// all UEs in the same notification share identical time ranges,
				// matching the live UPF-EES behavior.
				m = UsageMeasures{
					Key:            SessionKey{LocalSEID: uint64(wIdx + 1)}, // pseudo SEID
					ULBytesDelta:   ca.ulBytes,
					DLBytesDelta:   ca.dlBytes,
					ULPacketsDelta: ca.ulPkts,
					DLPacketsDelta: ca.dlPkts,
					StartTime:      defaultStartT,
					EndTime:        defaultEndT,
					UeIpv4Addr:     ueIP,
				}
			} else {
				// Pad with zero traffic if no packets appeared in this window
				m = UsageMeasures{
					Key:            SessionKey{LocalSEID: uint64(wIdx + 1)},
					ULBytesDelta:   0,
					DLBytesDelta:   0,
					ULPacketsDelta: 0,
					DLPacketsDelta: 0,
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

		// 1. Publish current Phase 2 window time for Kernel report alignment.
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

		// 2. Wait for TickOnce[N-1] to finish first.
		//    This ensures we don't push window N's data prematurely.
		waitStart := time.Now()
		pd.aggregator.WaitForTick()
		waitDuration := time.Since(waitStart)

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
