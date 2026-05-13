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

	// Batching mechanism for consecutive subscriptions
	batchMu      sync.Mutex
	pendingSubs  []*Subscription
	batchRunning bool
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

// ScheduleReplay adds a subscription to the next replay batch.
// If no batch is running, it starts a timer to coalesce consecutive requests.
func (pd *PseudoDriver) ScheduleReplay(sub *Subscription) {
	if pd == nil {
		return
	}
	pd.batchMu.Lock()
	defer pd.batchMu.Unlock()

	pd.pendingSubs = append(pd.pendingSubs, sub)
	pd.logger.WithField("subId", sub.ID).Info("pseudo driver: subscription scheduled for batch replay")

	if !pd.batchRunning {
		pd.batchRunning = true
		go pd.runBatchLoop()
	}
}

func (pd *PseudoDriver) runBatchLoop() {
	// Coalesce window: wait for more consecutive subscriptions
	time.Sleep(500 * time.Millisecond)

	pd.batchMu.Lock()
	subs := pd.pendingSubs
	pd.pendingSubs = nil
	pd.batchMu.Unlock()

	if len(subs) > 0 {
		pd.LoadAndReplayBatch(subs)
	}

	pd.batchMu.Lock()
	if len(pd.pendingSubs) > 0 {
		// New subs arrived while we were processing or sleeping
		go pd.runBatchLoop()
	} else {
		pd.batchRunning = false
	}
	pd.batchMu.Unlock()
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

type subContext struct {
	sub         *Subscription
	phase1Accum map[windowKey]*counterAccum
	phase2Accum map[windowKey]*counterAccum
	uniqueUEs   map[string]bool
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

// LoadAndReplayBatch reads the Parquet files and replays them for multiple subscriptions.
func (pd *PseudoDriver) LoadAndReplayBatch(subs []*Subscription) {
	if pd == nil || pd.parquetDir == "" || len(subs) == 0 {
		return
	}

	subIDs := make([]string, len(subs))
	for i, s := range subs {
		subIDs[i] = s.ID
	}

	pd.logger.WithFields(logrus.Fields{
		"subscriptionCount": len(subs),
		"subscriptionIds":   subIDs,
		"parquetDir":        pd.parquetDir,
	}).Info("pseudo driver starting warm-start batch replay stream")

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
			"error": err,
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
			"directory": pd.parquetDir,
		}).Warn("pseudo driver: no parquet files found in directory")
		return
	}

	sort.Strings(files)

	// Determine common alignment (using first sub's period for simplicity,
	// assuming they are identical in batch)
	periodSec := subs[0].PeriodSec
	if periodSec <= 0 {
		periodSec = 5 // default fallback
	}
	period := float64(periodSec)
	alignedBreakingTime := math.Ceil(breakingTimeSec/period) * period

	// =====================================================================
	// STREAMING AGGREGATION (Shared pass for all subs in batch)
	// =====================================================================
	pd.logger.Info("pseudo driver: streaming parquet files into aggregation maps")

	contexts := make([]*subContext, len(subs))
	for i, s := range subs {
		contexts[i] = &subContext{
			sub:         s,
			phase1Accum: make(map[windowKey]*counterAccum),
			phase2Accum: make(map[windowKey]*counterAccum),
			uniqueUEs:   make(map[string]bool),
		}
	}

	globalTimeOffset := 0.0
	var totalP1PktCount, totalP2PktCount int

	for _, fileObj := range files {
		minTS, maxTS, rowCount, scanErr := pd.scanTimestampRangeBatch(fileObj, subs)
		if scanErr != nil || rowCount == 0 {
			continue
		}

		p1, p2 := pd.streamAndAccumulateBatch(
			fileObj, contexts, minTS, globalTimeOffset,
			alignedBreakingTime, period,
		)
		totalP1PktCount += p1
		totalP2PktCount += p2
		globalTimeOffset += (maxTS - minTS) + 0.001
	}

	pd.logger.WithFields(logrus.Fields{
		"totalPhase1Packets": totalP1PktCount,
		"totalPhase2Packets": totalP2PktCount,
	}).Info("pseudo driver: shared streaming aggregation complete")

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

	for _, ctx := range contexts {
		ctx.sub.GridAnchor = referenceTime

		// Phase 1: Warmstart
		if len(ctx.phase1Accum) > 0 {
			endWIdx := int(alignedBreakingTime/period) - 1
			windows := pd.buildWindowsFromAccum(ctx.sub, ctx.phase1Accum, ctx.uniqueUEs, periodSec, referenceTime, endWIdx)
			ctx.phase1Accum = nil // GC
			for _, measures := range windows {
				logicalTime := referenceTime.Add(time.Duration(len(windows)*periodSec) * time.Second)
				if pd.aggregator != nil {
					pd.aggregator.PushHistoricalMeasures(ctx.sub, measures, logicalTime)
				}
			}
		} else {
			ctx.sub.LastNotify = anchorTime
		}

		ctx.sub.SimMu.Lock()
		ctx.sub.WarmupPending = false
		ctx.sub.SimMu.Unlock()
	}

	pd.logger.Info("pseudo driver: batch Phase 1 warmstart completed")

	// =====================================================================
	// PHASE 2: Parallel Future Simulation
	// =====================================================================
	var wg sync.WaitGroup
	for _, ctx := range contexts {
		if len(ctx.phase2Accum) > 0 && pd.aggregator != nil {
			startWIdx := int(alignedBreakingTime / period)
			endP2WIdx := 0
			for key := range ctx.phase2Accum {
				if key.windowIndex > endP2WIdx {
					endP2WIdx = key.windowIndex
				}
			}
			windows := pd.buildWindowsFromAccum(ctx.sub, ctx.phase2Accum, ctx.uniqueUEs, periodSec, referenceTime, endP2WIdx)
			ctx.phase2Accum = nil // GC

			wg.Add(1)
			go func(s *Subscription, w [][]UsageMeasures, idx int) {
				defer wg.Done()
				pd.simulateFutureRealTime(s, w, idx)
			}(ctx.sub, windows, startWIdx)
		}
	}
	wg.Wait()
}

// matchesAnySubscription checks if a UE IP matches any subscription in the batch.
func matchesAnySubscription(subs []*Subscription, ueIP string) bool {
	for _, s := range subs {
		if matchesSubscriptionFilter(s, ueIP) {
			return true
		}
	}
	return false
}

// scanTimestampRangeBatch finds min/max timestamps across a batch.
func (pd *PseudoDriver) scanTimestampRangeBatch(filePath string, subs []*Subscription) (minTS, maxTS float64, rowCount int, err error) {
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
		if !matchesAnySubscription(subs, row.UeIP) {
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

// streamAndAccumulateBatch streams a file once and dispatches rows to multiple sub contexts.
func (pd *PseudoDriver) streamAndAccumulateBatch(
	filePath string, contexts []*subContext,
	minTS, globalTimeOffset, alignedBreakingTime, period float64,
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

		globalTS := (row.Timestamp - minTS) + globalTimeOffset
		wIdx := int(math.Floor(globalTS / period))

		pkt := parsedPacket{
			ueIP:      row.UeIP,
			timestamp: globalTS,
			pktLen:    uint64(row.Len),
			isUplink:  row.Direction == "0",
		}

		isP1 := globalTS <= alignedBreakingTime

		for _, ctx := range contexts {
			if matchesSubscriptionFilter(ctx.sub, row.UeIP) {
				ctx.uniqueUEs[row.UeIP] = true
				key := windowKey{ueIP: row.UeIP, windowIndex: wIdx}
				if isP1 {
					accumulatePacket(ctx.phase1Accum, key, pkt)
				} else {
					accumulatePacket(ctx.phase2Accum, key, pkt)
				}
			}
		}

		if isP1 {
			phase1Count++
		} else {
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
				pd.logger.WithFields(logrus.Fields{
					"ue":      ueIP,
					"wIdx":    wIdx,
					"ulBytes": ca.ulBytes,
					"dlBytes": ca.dlBytes,
					"subId":   sub.ID,
				}).Debug("pseudo driver: window measures extracted from parquet")
			} else {
				m = UsageMeasures{
					Key:        SessionKey{LocalSEID: uint64(wIdx + 1)},
					StartTime:  defaultStartT,
					EndTime:    defaultEndT,
					UeIpv4Addr: ueIP,
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

// matchesSubscriptionFilter checks if a UE IP matches the subscription's target scope.
func matchesSubscriptionFilter(sub *Subscription, ueIP string) bool {
	if sub.Target.AnyUE {
		return true
	}
	return sub.Target.UeIPAddress != "" && sub.Target.UeIPAddress == ueIP
}

// LoadAndReplay is kept for backward compatibility but now calls ScheduleReplay.
func (pd *PseudoDriver) LoadAndReplay(sub *Subscription) {
	pd.ScheduleReplay(sub)
}
