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
	"github.com/sirupsen/logrus"
)

// PseudoDriver reads Parquet files of historical per-packet traffic data
// and replays them as EES notifications to provide a warm start for subscribers.
type PseudoDriver struct {
	parquetDir string
	notifier   *Notifier
	logger     *logrus.Entry

	aggregator *Aggregator

	// First URR Signal synchronization: broadcast to ALL Phase 1 threads
	firstURRReady chan struct{}
	firstURRTime  time.Time
	firstURROnce  sync.Once
}

// NewPseudoDriver constructs a PseudoDriver.
func NewPseudoDriver(parquetDir string, notifier *Notifier, logger *logrus.Entry) *PseudoDriver {
	return &PseudoDriver{
		parquetDir:    parquetDir,
		notifier:      notifier,
		logger:        logger,
		firstURRReady: make(chan struct{}),
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
		pd.firstURRTime = urrStartTime
		close(pd.firstURRReady)
		pd.logger.WithField("urrStartTime", urrStartTime).Info("pseudo driver: first URR signal broadcasted")
	})
}

// ParquetRow represents a single row from the historical traffic Parquet file.
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

	pd.logger.WithFields(logrus.Fields{
		"subscriptionId": sub.ID,
		"parquetDir":     pd.parquetDir,
		"periodSec":      sub.PeriodSec,
	}).Info("pseudo driver starting warm-start replay stream")

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

	pd.logger.WithField("breakingTimeSec", breakingTimeSec).Info("pseudo driver: using breaking time")

	// 1. Scan directory for Parquet files
	entries, err := os.ReadDir(pd.parquetDir)
	if err != nil {
		pd.logger.WithFields(logrus.Fields{
			"subscriptionId": sub.ID,
			"error":          err,
		}).Error("pseudo driver failed to read parquet directory")
		return
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > 8 && entry.Name()[len(entry.Name())-8:] == ".parquet" {
			files = append(files, pd.parquetDir+"/"+entry.Name())
		}
	}

	if len(files) == 0 {
		pd.logger.WithFields(logrus.Fields{
			"subscriptionId": sub.ID,
			"directory":      pd.parquetDir,
		}).Warn("pseudo driver: no parquet files found in directory")
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
	// =====================================================================
	pd.logger.Info("pseudo driver: streaming parquet files into aggregation maps")
	phase1Accum := make(map[windowKey]*counterAccum)
	phase2Accum := make(map[windowKey]*counterAccum)
	uniqueUEs := make(map[string]bool)
	globalTimeOffset := 0.0
	var phase1PktCount, phase2PktCount int

	for _, fileObj := range files {
		minTS, maxTS, rowCount, scanErr := pd.scanTimestampRange(fileObj, sub)
		if scanErr != nil || rowCount == 0 {
			continue
		}

		p1, p2 := pd.streamAndAccumulate(
			fileObj, sub, minTS, globalTimeOffset,
			alignedBreakingTime, period,
			phase1Accum, phase2Accum, uniqueUEs,
		)
		phase1PktCount += p1
		phase2PktCount += p2
		globalTimeOffset += (maxTS - minTS) + 0.001
	}

	pd.logger.WithFields(logrus.Fields{
		"phase1Entries": len(phase1Accum),
		"phase2Entries": len(phase2Accum),
		"phase1Packets": phase1PktCount,
		"phase2Packets": phase2PktCount,
		"uniqueUEs":     len(uniqueUEs),
	}).Info("pseudo driver: streaming aggregation complete")

	// Wait for the first URR report from the kernel to anchor the timeline.
	var anchorTime time.Time
	const urrWaitTimeout = 60 * time.Second
	pd.logger.WithField("timeout", urrWaitTimeout).Info("pseudo driver: waiting for first URR signal to anchor timeline")

	select {
	case <-pd.firstURRReady:
		urrTime := pd.firstURRTime
		pd.logger.Info("pseudo driver: first URR received, anchoring timeline instantly")
		anchorTime = urrTime.Round(time.Second) // Align to clean second boundary
	case <-time.After(urrWaitTimeout):
		anchorTime = time.Now()
		pd.logger.WithField("timeout", urrWaitTimeout).Warn("pseudo driver: URR wait timed out, falling back to time.Now()")
	}

	// Absolute reference time for Parquet offset 0
	referenceTime := anchorTime.Add(-time.Duration(alignedBreakingTime * float64(time.Second)))

	// CRITICAL: Set the subscription's GridAnchor to our referenceTime.
	sub.GridAnchor = referenceTime
	pd.logger.WithFields(logrus.Fields{
		"gridAnchor":           referenceTime,
		"anchorTime":           anchorTime,
		"alignedBreakingTime": alignedBreakingTime,
	}).Info("pseudo driver: grid alignment established")

	// =====================================================================
	// Build Phase 2 windows from pre-aggregated accum map.
	// =====================================================================
	var phase2Windows [][]UsageMeasures
	if len(phase2Accum) > 0 {
		endP2WIdx := 0
		for key := range phase2Accum {
			if key.windowIndex > endP2WIdx {
				endP2WIdx = key.windowIndex
			}
		}
		phase2Windows = pd.buildWindowsFromAccum(sub, phase2Accum, uniqueUEs, periodSec, referenceTime, endP2WIdx)
		phase2Accum = nil // Allow GC to reclaim
	}

	// =====================================================================
	// PHASE 1: Warmstart (Historical Burst - Near Instant)
	// =====================================================================
	pd.logger.WithField("packets", phase1PktCount).Info("pseudo driver: executing Phase 1 (Warmstart)")

	if len(phase1Accum) > 0 {
		endWIdx := int(alignedBreakingTime/period) - 1
		windows := pd.buildWindowsFromAccum(sub, phase1Accum, uniqueUEs, periodSec, referenceTime, endWIdx)
		phase1Accum = nil // Allow GC to reclaim
		for _, measures := range windows {
			logicalTime := referenceTime.Add(time.Duration(len(windows)*periodSec) * time.Second)
			if pd.aggregator != nil {
				pd.aggregator.PushHistoricalMeasures(sub, measures, logicalTime)
			}
		}
	} else {
		sub.LastNotify = anchorTime
	}

	        sub.SimMu.Lock()
        sub.WarmupPending = false
        sub.SimMu.Unlock()
        pd.logger.WithField("subId", sub.ID).Info("pseudo driver: Phase 1 warmstart completed, clearing WarmupPending flag")

        // =====================================================================
	// PHASE 2: Parallel Future Simulation (Synchronized pacing)
	// =====================================================================
	if len(phase2Windows) > 0 && pd.aggregator != nil {
		startWIdx := int(alignedBreakingTime / period)
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

// scanTimestampRange reads through a Parquet file to find the min/max timestamps.
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

// streamAndAccumulate re-reads a Parquet file and accumulates each matching row.
func (pd *PseudoDriver) streamAndAccumulate(
	filePath string, sub *Subscription,
	minTS, globalTimeOffset, alignedBreakingTime, period float64,
	phase1Accum, phase2Accum map[windowKey]*counterAccum,
	uniqueUEs map[string]bool,
) (phase1Count, phase2Count int) {
	f, err := os.Open(filePath)
	if err != nil {
		pd.logger.WithFields(logrus.Fields{
			"file":  filePath,
			"error": err,
		}).Error("pseudo driver: failed to re-open parquet file")
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
func (pd *PseudoDriver) buildWindowsFromAccum(
	sub *Subscription,
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

	// Build output: one []UsageMeasures per window
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
		sub.SimMu.Lock()
		sub.IsSimulating = false
		sub.SimMu.Unlock()
		pd.logger.WithField("subId", sub.ID).Info("pseudo driver: Phase 2 simulation ended for subscription")
	}()

	sub.SimMu.Lock()
	sub.IsSimulating = true
	sub.SimMu.Unlock()

	pd.logger.WithFields(logrus.Fields{
		"subId":         sub.ID,
		"windowsToPace": len(windows) - startWIdx,
	}).Info("pseudo driver: Phase 2 future simulation pacing loop started")

	aggPeriodSec := int(pd.aggregator.reportPeriod.Seconds())
	if aggPeriodSec <= 0 {
		aggPeriodSec = 5
	}
	ticksPerWindow := sub.PeriodSec / aggPeriodSec
	if ticksPerWindow < 1 {
		ticksPerWindow = 1
	}

	for i := startWIdx; i < len(windows); i++ {
		winMeasures := windows[i]

				if len(winMeasures) > 0 && pd.aggregator != nil {
			pd.logger.WithFields(logrus.Fields{
				"subId":         sub.ID,
				"windowIndex":   i,
				"measures":      len(winMeasures),
				"dataStartTime": winMeasures[0].StartTime,
				"dataEndTime":   winMeasures[0].EndTime,
			}).Info("pseudo driver: pushing Phase 2 data")
			pd.aggregator.PushLiveMeasures(sub, winMeasures)
		}

                // Catch-up logic: if real time has already passed this window's EndTime, skip waiting
                windowEnd := sub.GridAnchor.Add(time.Duration((i+1)*sub.PeriodSec) * time.Second)
		if time.Now().After(windowEnd) {
                        pd.logger.Debug("pseudo driver: lagging behind real time, skipping tick wait to catch up")
                        continue
                }

// Wait for the correct number of Aggregator heartbeats
		for t := 0; t < ticksPerWindow; t++ {
			pd.aggregator.WaitForTick()
		}
	}

	pd.logger.WithField("subId", sub.ID).Info("pseudo driver: Phase 2 future simulation completed for subscription")
}
