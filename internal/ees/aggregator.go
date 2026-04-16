// Package ees - UPF Event Exposure Service (EES)
// aggregator.go: periodic/on-demand reporting loop for USER_DATA_USAGE_MEASURES.
//
// MVP behavior (interval semantics):
// - Source.SnapshotNow() returns per-session counters for the provider's current interval
//   (UL/DL bytes/packets with StartTime/EndTime).
// - Aggregator uses the current interval values as-is (no subtraction from previous snapshots).
// - Throughput is derived as (bytes * 8) / (EndTime - StartTime) in seconds, with guards.
// - "Clean up unused source": after each tick, for every subscription, remove entries in
//   subscription.Snapshots whose SessionKey is absent in the current snapshot keys.
// - ModeOnDemand: deliver one immediate report using the current interval data, then refresh
//   snapshots and switch the subscription.Mode to PERIODIC.

package ees

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/free5gc/go-upf/internal/report"
)

// Aggregator accumulates usage reports pushed from the kernel and periodically
// sends notifications to subscribers. This is a pure Push model - no active polling.
// PerioServerInterface provides access to URR period information
type PerioServerInterface interface {
	GetAnyURRPeriod(urrid uint32) time.Duration
}

type Aggregator struct {
	subscriptionStore *SubscriptionStore
	reportPeriod      time.Duration
	notifier          *Notifier
	logger            *zap.Logger

	// [Push Mode] Mutex protects reportBuffer
	mu sync.Mutex
	// [Push Mode] Accumulated reports per subscription: Key=SubscriptionID
	reportBuffer map[string][]UsageMeasures

	sessionProvider SessionProvider      // Added: to lookup UE IP
	perioServer     PerioServerInterface // Added: to query URR periods
	pseudoDriver    *PseudoDriver        // Signal pseudo driver on first URR

	// Dynamic period adjustment
	ticker         *time.Ticker
	tickerMu       sync.Mutex
	tickerReset    chan struct{} // Signal to reset ticker when period changes
	periodAdjusted bool          // Marks if period has been adjusted once

	// Shared ticker synchronization: Phase 2 waits on tickDone after each TickOnce
	tickDone     chan struct{} // Signal Phase 2 after TickOnce completes
	lastTickTime time.Time     // Wall-clock time of most recent TickOnce
	lastTickMu   sync.RWMutex

	// Kernel Ticker synchronization: align Aggregator heartbeat with Kernel
	firstURRReceived bool // Marks if the first real Kernel URR has arrived
	startMu          sync.Mutex

	// Signal-driven TickOnce: PushReport sends this after buffering Kernel data.
	// Run loop waits for this signal (or a safety timeout) instead of a fixed sleep,
	// ensuring TickOnce fires immediately after the CURRENT period's Kernel data
	// arrives, without waiting so long that the NEXT period's data sneaks in.
	kernelReady chan struct{}

	// [Legacy] State cache: kept for delta computation if needed
	// Key: SessionKey, Value: Last Counters
	lastSnapshot map[SessionKey]Counters
	// [Legacy] Record the time when the last Snapshot occurred
	lastSnapshotTime time.Time
}

// NewAggregator constructs an Aggregator for pure Push mode.
// Reports are accumulated via PushReport() and sent periodically by TickOnce().
func NewAggregator(
	subscriptionStore *SubscriptionStore,
	reportPeriod time.Duration,
	notifier *Notifier,
	logger *zap.Logger,
	sessionProvider SessionProvider,
	perioServer PerioServerInterface,
	pseudoDriver *PseudoDriver,
) *Aggregator {
	return &Aggregator{
		subscriptionStore: subscriptionStore,
		reportPeriod:      reportPeriod,
		notifier:          notifier,
		logger:            logger,
		sessionProvider:   sessionProvider,
		perioServer:       perioServer,
		pseudoDriver:      pseudoDriver,

		// Initialize report buffer for Push mode
		reportBuffer: make(map[string][]UsageMeasures),

		// Initialize ticker reset channel
		tickerReset: make(chan struct{}, 1),

		// Initialize kernel-ready signal channel (buffered=1 to avoid blocking PushReport)
		kernelReady: make(chan struct{}, 1),

		// Initialize shared ticker synchronization channel
		tickDone: make(chan struct{}, 1),

		// Initialize legacy snapshot maps (kept for potential future use)
		lastSnapshot:     make(map[SessionKey]Counters),
		lastSnapshotTime: time.Now(),
	}
}

// Run starts the periodic loop until ctx is done.
// Uses a dual-loop structure to handle ticker replacement when period is adjusted.
func (aggregator *Aggregator) Run(parentContext context.Context) {
	aggregator.logger.Info("ees aggregator started",
		zap.Duration("reportPeriod", aggregator.reportPeriod),
	)

	// Outer loop: recreate ticker when reset signal is received
	for {
		// Create/recreate ticker with current period
		aggregator.tickerMu.Lock()
		if aggregator.ticker != nil {
			aggregator.ticker.Stop()
		}
		aggregator.ticker = time.NewTicker(aggregator.reportPeriod)
		tickerC := aggregator.ticker.C // Capture channel before unlocking
		currentPeriod := aggregator.reportPeriod
		aggregator.tickerMu.Unlock()

		aggregator.logger.Debug("ees ticker initialized",
			zap.Duration("period", currentPeriod),
		)

		// Inner loop: process ticks until reset or shutdown
	tickerLoop:
		for {
			select {
			case <-parentContext.Done():
				aggregator.tickerMu.Lock()
				if aggregator.ticker != nil {
					aggregator.ticker.Stop()
				}
				aggregator.tickerMu.Unlock()
				aggregator.logger.Info("ees aggregator stopped")
				return
			case <-aggregator.tickerReset:
				// Ticker needs to be recreated with new period
				aggregator.logger.Info("ees ticker reset signal received, recreating ticker")
				break tickerLoop
			case <-tickerC:
				// SIGNAL-DRIVEN WAIT (Solution B):
				// Instead of a fixed sleep, we wait for PushReport to signal that
				// the CURRENT period's Kernel data has arrived. This ensures we
				// fire TickOnce as soon as the data is ready, without waiting so
				// long that the NEXT period's URR sneaks into the buffer.
				//
				// Safety timeout (2s) handles edge cases:
				// - No active Kernel sessions (pure Pseudo mode)
				// - PushReport signal lost due to race condition
				select {
				case <-aggregator.kernelReady:
					// Small buffer to let all sessions' PushReport complete
					time.Sleep(50 * time.Millisecond)
					aggregator.logger.Debug("ees kernel-ready signal received, proceeding to TickOnce")
				case <-time.After(2 * time.Second):
					aggregator.logger.Debug("ees kernel-ready timeout, proceeding to TickOnce")
				}

				// Record tick time BEFORE processing
				// CRITICAL FIX: time.Now() has millisecond/microsecond jitter (e.g. .663Z).
				// We MUST snap this to a perfect multiple of currentPeriod (e.g. .000Z)
				// so the Phase 2 simulation grid doesn't inherit a random sub-second offset,
				// which causes math.Round() in snapTime() to sometimes round up and sometimes round down
				// for the exact same conceptual window.
				aggregator.lastTickMu.Lock()
				nowRaw := time.Now()
				if aggregator.lastTickTime.IsZero() {
					// First tick: strip the sub-second fraction OR align precisely to Kernel's arrival
					aggregator.lastTickTime = nowRaw.Truncate(time.Second)
				} else {
					// Subsequent ticks: rigidly advance by exactly one period.
					// This completely ignores Ticker jitter and OS scheduling delays,
					// ensuring the grid remains perfectly mathematically aligned forever.
					aggregator.lastTickTime = aggregator.lastTickTime.Add(currentPeriod).Truncate(time.Second)
				}
				aggregator.lastTickMu.Unlock()

				if _, err := aggregator.TickOnce(parentContext); err != nil {
					aggregator.logger.Warn("ees aggregator tick failed", zap.Error(err))
				}

				// Signal Phase 2 that this tick cycle is done
				select {
				case aggregator.tickDone <- struct{}{}:
				default:
				}
			}
		}
	}
}

// getTicker safely retrieves the current ticker
func (aggregator *Aggregator) getTicker() *time.Ticker {
	aggregator.tickerMu.Lock()
	defer aggregator.tickerMu.Unlock()
	return aggregator.ticker
}

// WaitForTick blocks until the next TickOnce completes.
// Used by Phase 2 to synchronize with Aggregator's heartbeat.
func (aggregator *Aggregator) WaitForTick() {
	<-aggregator.tickDone
}

// DrainTickDone removes any stale tickDone signal from the channel.
// Must be called before Phase 2's pacing loop to prevent the first
// WaitForTick() from returning immediately due to a signal that was
// sent during Phase 1 historical replay.
func (aggregator *Aggregator) DrainTickDone() {
	select {
	case <-aggregator.tickDone:
	default:
	}
}

// GetLastTickTime returns the wall-clock time of the most recent TickOnce trigger.
// Used by PseudoDriver to anchor the time grid to the Aggregator's heartbeat.
func (aggregator *Aggregator) GetLastTickTime() time.Time {
	aggregator.lastTickMu.RLock()
	defer aggregator.lastTickMu.RUnlock()
	return aggregator.lastTickTime
}

// TickOnce sends notifications using accumulated reports from the Push buffer.
// This is the Pure Push model - no active polling.
// Reports for the same session are consolidated into a single report.
// Each subscription's reportPeriod is respected - notifications are only sent
// when enough time has passed since the last notification.
// Returns number of notifications attempted (sum over all subscriptions) and any error.
func (aggregator *Aggregator) TickOnce(ctx context.Context) (int, error) {
	_ = ctx // ctx reserved for future cancellation support

	now := time.Now()
	totalNotifications := 0

	// Lock and swap out the buffer to process current batch
	// This prevents memory leak by clearing the buffer after taking a snapshot
	aggregator.mu.Lock()
	bufferedReports := aggregator.reportBuffer
	aggregator.reportBuffer = make(map[string][]UsageMeasures)
	// Create a new buffer for reports that should be kept (period not elapsed)
	newBuffer := make(map[string][]UsageMeasures)
	aggregator.mu.Unlock()

	// Iterate over all subscriptions
	subscriptions := aggregator.subscriptionStore.AllSubscriptions()

	aggregator.logger.Info("ees tick starting",
		zap.Int("subscriptionCount", len(subscriptions)),
		zap.Time("currentTime", now),
	)

	for _, subscription := range subscriptions {
		// MVP scope: only USER_DATA_USAGE_MEASURES + perPduSession
		if subscription.Event != EventUserDataUsageMeasures ||
			subscription.Granularity != GranularityPerSession {
			aggregator.logger.Debug("ees aggregator skip unsupported subscription",
				zap.String("subscriptionId", subscription.ID),
				zap.String("event", string(subscription.Event)),
				zap.String("granularity", string(subscription.Granularity)),
			)
			continue
		}

		// Get accumulated reports for this subscription
		usageMeasuresList, hasReports := bufferedReports[subscription.ID]

		if !hasReports || len(usageMeasuresList) == 0 {
			aggregator.logger.Debug("ees tick - no reports for subscription",
				zap.String("subscriptionId", subscription.ID),
			)
			continue
		}

		aggregator.logger.Info("ees tick - subscription has reports",
			zap.String("subscriptionId", subscription.ID),
			zap.Int("reportCount", len(usageMeasuresList)),
			zap.String("mode", string(subscription.Mode)),
			zap.Int("periodSec", subscription.PeriodSec),
		)

		// Check if enough time has passed since last notification (respect subscription's reportPeriod)
		// For ON_DEMAND mode, always send immediately
		if subscription.Mode == ModePeriodic {
			periodDuration := time.Duration(subscription.PeriodSec) * time.Second
			timeSinceLastNotify := now.Sub(subscription.LastNotify)

			// Add tolerance to absorb timing jitter from:
			// 1. The 100ms sleep we added before TickOnce (to wait for Kernel reports)
			// 2. OS/Go runtime scheduling delays
			// 3. Ticker precision variations
			// 1000ms is safe because Ticker guarantees 5s gaps.
			const tolerance = 1000 * time.Millisecond

			if timeSinceLastNotify+tolerance < periodDuration {
				// Not time to notify yet - keep reports in buffer for next tick
				aggregator.mu.Lock()
				newBuffer[subscription.ID] = append(newBuffer[subscription.ID], usageMeasuresList...)
				aggregator.mu.Unlock()

				remainingTime := periodDuration - timeSinceLastNotify
				aggregator.logger.Info("ees buffering reports - period not elapsed",
					zap.String("subscriptionId", subscription.ID),
					zap.Duration("elapsed", timeSinceLastNotify),
					zap.Duration("periodRequired", periodDuration),
					zap.Duration("remaining", remainingTime),
					zap.Int("bufferedReports", len(usageMeasuresList)),
				)
				continue
			}

			aggregator.logger.Info("ees ready to send notification - period elapsed",
				zap.String("subscriptionId", subscription.ID),
				zap.Duration("timeSinceLastNotify", timeSinceLastNotify),
				zap.Duration("periodRequired", periodDuration),
			)
		}

		periodDuration := time.Duration(subscription.PeriodSec) * time.Second

		// 1. Time Window Snapping (Before Consolidation)
		for i := range usageMeasuresList {
			oldStart := usageMeasuresList[i].StartTime
			oldEnd := usageMeasuresList[i].EndTime
			if !subscription.GridAnchor.IsZero() {
				snappedEnd := snapTime(usageMeasuresList[i].EndTime, subscription.GridAnchor, periodDuration)
				usageMeasuresList[i].EndTime = snappedEnd
				usageMeasuresList[i].StartTime = snappedEnd.Add(-periodDuration)
			} else {
				snappedEnd := usageMeasuresList[i].EndTime.Round(periodDuration)
				usageMeasuresList[i].EndTime = snappedEnd
				usageMeasuresList[i].StartTime = snappedEnd.Add(-periodDuration)
			}
			aggregator.logger.Debug("ees snap result",
				zap.Int("idx", i),
				zap.Time("oldEnd", oldEnd),
				zap.Time("newEnd", usageMeasuresList[i].EndTime),
				zap.Time("oldStart", oldStart),
				zap.Time("newStart", usageMeasuresList[i].StartTime),
			)
		}

		// 2. Consolidate reports: group by (IP + SnappedStartTime)
		consolidatedList := consolidateReports(usageMeasuresList)

		// 3. Compute throughput for the final consolidated list
		for i := range consolidatedList {
			computeThroughputIfPossible(&consolidatedList[i])
		}

		aggregator.logger.Info("ees consolidation result",
			zap.String("subscriptionId", subscription.ID),
			zap.Int("beforeCount", len(usageMeasuresList)),
			zap.Int("afterCount", len(consolidatedList)),
		)

		if subscription.Mode == ModeOnDemand {
			if err := aggregator.notifier.Notify(subscription, consolidatedList); err != nil {
				aggregator.logger.Warn("ees notify (on-demand) failed",
					zap.String("subscriptionId", subscription.ID),
					zap.Error(err),
					zap.Int("items", len(consolidatedList)),
				)
			} else {
				totalNotifications++
				subscription.LastNotify = now
				aggregator.logger.Debug("ees notify (on-demand) success",
					zap.String("subscriptionId", subscription.ID),
					zap.Int("items", len(consolidatedList)),
				)
			}
			// Switch to Periodic after OnDemand processing
			subscription.Mode = ModePeriodic
			continue
		}

		// PERIODIC mode - send notification
		aggregator.logger.Info("ees sending notification",
			zap.String("subscriptionId", subscription.ID),
			zap.String("notifyUri", subscription.NotifURI),
			zap.Int("items", len(consolidatedList)),
		)

		if err := aggregator.notifier.Notify(subscription, consolidatedList); err != nil {
			aggregator.logger.Warn("ees notify (periodic) failed",
				zap.String("subscriptionId", subscription.ID),
				zap.Error(err),
				zap.Int("items", len(consolidatedList)),
			)
		} else {
			totalNotifications++
			subscription.LastNotify = now
			aggregator.logger.Info("ees notify (periodic) success",
				zap.String("subscriptionId", subscription.ID),
				zap.Int("items", len(consolidatedList)),
				zap.Int("periodSec", subscription.PeriodSec),
				zap.Time("nextNotifyAfter", now.Add(time.Duration(subscription.PeriodSec)*time.Second)),
			)
		}
	}

	// Update the buffer with reports that were kept (for subscriptions whose period hasn't elapsed)
	aggregator.mu.Lock()
	for subID, reports := range newBuffer {
		aggregator.reportBuffer[subID] = append(aggregator.reportBuffer[subID], reports...)
	}
	aggregator.mu.Unlock()

	aggregator.lastSnapshotTime = now
	return totalNotifications, nil
}

func snapTime(t time.Time, anchor time.Time, period time.Duration) time.Time {
	offset := t.Sub(anchor)
	// Round offset to the nearest multiple of period using pure integer math
	// to avoid float64 precision errors that can cause 1-nanosecond differences.
	halfPeriod := period / 2
	var roundedOffset time.Duration
	if offset >= 0 {
		roundedOffset = ((offset + halfPeriod) / period) * period
	} else {
		roundedOffset = ((offset - halfPeriod) / period) * period
	}

	// Truncate to second to definitively wipe out any sub-second noise
	return anchor.Add(roundedOffset).Truncate(time.Second)
}

func (aggregator *Aggregator) PushHistoricalMeasures(sub *Subscription, measures []UsageMeasures, logicalTime time.Time) {
	if len(measures) == 0 {
		return
	}
	// By user design: Place historical data into the shared buffer instead of
	// sending immediately. This guarantees that Phase 1 historical data will
	// perfectly merge with any overlapping real-time URR data via the native
	// consolidateReports() logic during the next TickOnce.
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()
	aggregator.reportBuffer[sub.ID] = append(aggregator.reportBuffer[sub.ID], measures...)
}

func (aggregator *Aggregator) PushLiveMeasures(sub *Subscription, measures []UsageMeasures) {
	if len(measures) == 0 {
		return
	}

	// CRITICAL FIX: Ticker Re-sync Mechanism
	// We want the Aggregator's timer to be perfectly out-of-phase with the
	// Kernel's perio server (which generates these measures). By resetting
	// our Ticker exactly when the FIRST kernel measure arrives, we guarantee
	// our future ticks will fire exactly 5s after Kernel does + 500ms jitter sleep.
	// This ensures we NEVER check the buffer 8ms BEFORE Kernel drops the info.
	aggregator.startMu.Lock()
	if !aggregator.firstURRReceived {
		aggregator.firstURRReceived = true
		aggregator.logger.Info("ees aggregator received first live URR, forcing Ticker re-sync",
			zap.String("subscriptionId", sub.ID),
			zap.Time("arrival_time", time.Now()),
		)
		// Non-blocking trigger to recreate the Ticker in Run loop
		select {
		case aggregator.tickerReset <- struct{}{}:
		default:
		}
	}
	aggregator.startMu.Unlock()

	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()
	aggregator.reportBuffer[sub.ID] = append(aggregator.reportBuffer[sub.ID], measures...)
}

// consolidateReports merges multiple UsageMeasures by grouping them by BOTH IP and StartTime.
// Since StartTime is already snapped to the grid before this is called,
// Kernel and Phase 2 reports for the SAME window will merge perfectly,
// while reports for DIFFERENT windows will remain separate.
func consolidateReports(reports []UsageMeasures) []UsageMeasures {
	if len(reports) <= 1 {
		return reports
	}

	// Map by (UE IP + StartTime)
	consolidated := make(map[string]*UsageMeasures)

	for _, r := range reports {
		key := r.UeIpv4Addr
		if key == "" {
			// Fallback if IP is missing
			key = fmt.Sprintf("seid-%d-%d", r.Key.LocalSEID, r.Key.RemoteSEID)
		}

		// Group by both IP and snapped StartTime (rounded to nearest second)
		timeKey := fmt.Sprintf("%s-%d", key, r.StartTime.Unix())

		existing, ok := consolidated[timeKey]
		if !ok {
			// First occurrence - create a copy
			rCopy := r
			consolidated[timeKey] = &rCopy
			continue
		}

		// Update logic for Periodic Incremental Reports:
		// 1. Maintain the widest time window for correctness
		if r.StartTime.Before(existing.StartTime) {
			existing.StartTime = r.StartTime
		}
		if r.EndTime.After(existing.EndTime) {
			existing.EndTime = r.EndTime
		}

		// 2. Sum Volume/Packet counters
		existing.ULBytesDelta += r.ULBytesDelta
		existing.DLBytesDelta += r.DLBytesDelta
		existing.ULPacketsDelta += r.ULPacketsDelta
		existing.DLPacketsDelta += r.DLPacketsDelta
	}

	// Convert map back to slice
	result := make([]UsageMeasures, 0, len(consolidated))
	for _, m := range consolidated {
		result = append(result, *m)
	}

	// Sort results by StartTime ascending so notifications emerge in causal order
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].StartTime.Before(result[j].StartTime)
	})

	return result
}

// PushReport handles unsolicited reports (e.g. from Kernel via Handler).
// Reports are accumulated in reportBuffer and sent during the next TickOnce().
// New logic: Aggregate all SMF URRs per session (no Shadow URR filtering).
func (aggregator *Aggregator) PushReport(sessRpt report.SessReport) {
	// Note: With the shared ticker architecture, Phase 2 and TickOnce are perfectly
	// synchronized. Kernel reports flow naturally into the buffer and are merged
	// with Phase 2 data by consolidateReports (keyed by IP + SnappedStartTime).

	// Debug: Log all incoming reports
	aggregator.logger.Info("PushReport called",
		zap.Uint64("seid", sessRpt.SEID),
		zap.Int("totalReports", len(sessRpt.Reports)),
	)
	for i, r := range sessRpt.Reports {
		if r.Type() == report.USAR {
			if usarep, ok := r.(report.USAReport); ok {
				aggregator.logger.Info("PushReport - USAReport details",
					zap.Int("index", i),
					zap.Uint32("urrid", usarep.URRID),
					zap.Uint64("ulBytes", usarep.VolumMeasure.UplinkVolume),
					zap.Uint64("dlBytes", usarep.VolumMeasure.DownlinkVolume),
				)
			}
		}
	}

	// Get session context for UE IP lookup
	var ueIpv4Addr string
	if aggregator.sessionProvider != nil {
		contexts := aggregator.sessionProvider.GetSessionContexts()
		if ctx, ok := contexts[sessRpt.SEID]; ok {
			ueIpv4Addr = ctx.UeIPv4Addr
		}
	}

	// Use URR 2 (MAQE) as the single source for PER_PDU_SESSION measurements
	var totalUL, totalDL, totalULPkt, totalDLPkt uint64
	var startTime, endTime time.Time
	var urrIDs []uint32
	reportCount := 0

	for _, r := range sessRpt.Reports {
		if r.Type() != report.USAR {
			continue
		}
		usarep, ok := r.(report.USAReport)
		if !ok {
			continue
		}

		// Filter: Only use URR 2 (N3N6_MAQE - Measurement After QoS Enforcement)
		// URR 2 represents actual transmitted traffic for the entire PDU session
		// This is the perfect source for PER_PDU_SESSION granularity measurements
		if usarep.URRID != 2 {
			continue
		}

		// Aggregate volumes from all URRs
		totalUL += usarep.VolumMeasure.UplinkVolume
		totalDL += usarep.VolumMeasure.DownlinkVolume
		totalULPkt += usarep.VolumMeasure.UplinkPktNum
		totalDLPkt += usarep.VolumMeasure.DownlinkPktNum

		// Track time range: earliest StartTime, latest EndTime
		if reportCount == 0 || usarep.StartTime.Before(startTime) {
			startTime = usarep.StartTime
		}
		if reportCount == 0 || usarep.EndTime.After(endTime) {
			endTime = usarep.EndTime
		}

		urrIDs = append(urrIDs, usarep.URRID)
		reportCount++
	}

	// No usage reports to process
	if reportCount == 0 {
		return
	}

	// Build aggregated measure for this session
	m := UsageMeasures{
		Key:            SessionKey{LocalSEID: sessRpt.SEID},
		ULBytesDelta:   totalUL,
		DLBytesDelta:   totalDL,
		ULPacketsDelta: totalULPkt,
		DLPacketsDelta: totalDLPkt,
		StartTime:      startTime,
		EndTime:        endTime,
		UeIpv4Addr:     ueIpv4Addr,
	}
	computeThroughputIfPossible(&m)

	// Match to subscriptions by target scope
	subscriptions := aggregator.subscriptionStore.AllSubscriptions()
	for _, sub := range subscriptions {
		if !aggregator.matchesSubscription(sub, ueIpv4Addr) {
			continue
		}

		// Anchoring: Signal the PseudoDriver that the first real URR has arrived.
		// We use time.Now() (NOT m.EndTime!) because m.EndTime reflects the Kernel's
		// perio timer fire time. Passing time.Now() ensures the Phase 2 grid aligns
		// perfectly with the CURRENT moment.
		// CRITICAL: MUST BE INSIDE THE SUBSCRIPTION LOOP so we don't prematurely
		// signal the PseudoDriver before a subscription is even active!
		if aggregator.pseudoDriver != nil {
			aggregator.pseudoDriver.SignalFirstURR(time.Now())
		}

		if sub.GridAnchor.IsZero() {
			// Only set GridAnchor from Kernel if PseudoDriver has NOT set it.
			// When PseudoDriver is active, IT is the single source of truth for the grid.
			if aggregator.pseudoDriver == nil {
				sub.GridAnchor = m.EndTime
				aggregator.logger.Info("ees established grid anchor (vanilla mode)",
					zap.String("subscriptionId", sub.ID),
					zap.Time("gridAnchor", sub.GridAnchor),
				)
			} else {
				// During testing (e.g. Daisy), subscriptions are created and destroyed dynamically
				// mid-simulation. PseudoDriver cannot foresee this. So when the first Kernel 
				// report arrives on a new sub during Phase 2, we must anchor it perfectly 
				// to the Phase 2 mathematical grid.
				_, p2End, _ := aggregator.pseudoDriver.GetPhase2Window()
				sub.GridAnchor = p2End
				aggregator.logger.Info("ees established grid anchor dynamically aligned with Phase 2",
					zap.String("subscriptionId", sub.ID),
					zap.Time("gridAnchor", sub.GridAnchor),
				)
			}
		}

		// Prevent double-counting historical traffic already simulated by Phase 1 Parquet replay.
		// If the kernel report StartTime is significantly before our anchor (e.g. established PDU session),
		// we skip appending it to the buffer so it won't distort the live monitoring start-time and volume.
		if m.StartTime.Before(sub.GridAnchor.Add(-2 * time.Second)) {
			aggregator.logger.Info("ees dropping huge historical kernel burst to align with simulation",
				zap.String("subscriptionId", sub.ID),
				zap.Time("startTime", m.StartTime),
				zap.Time("anchorTime", sub.GridAnchor),
				zap.Uint64("volume", m.ULBytesDelta+m.DLBytesDelta),
			)
			continue
		}

		// DEBUG: Log Phase 2 alignment state and rewrite timestamps BEFORE buffering
		if aggregator.pseudoDriver != nil {
			p2Start, p2End, simActive := aggregator.pseudoDriver.GetPhase2Window()
			
			if simActive {
				m.StartTime = p2Start
				m.EndTime = p2End
			}
			
			aggregator.logger.Info("ees PushReport: Kernel report buffered during pseudo mode",
				zap.String("subscriptionId", sub.ID),
				zap.Bool("phase2Active", simActive),
				zap.Time("kernelStartTime", m.StartTime),
				zap.Time("kernelEndTime", m.EndTime),
				zap.Time("phase2WindowStart", p2Start),
				zap.Time("phase2WindowEnd", p2End),
				zap.Time("gridAnchor", sub.GridAnchor),
				zap.Time("wallClockNow", time.Now()),
			)
		}

		// Buffer everything directly; TickOnce will snap it
		aggregator.mu.Lock()
		aggregator.reportBuffer[sub.ID] = append(aggregator.reportBuffer[sub.ID], m)
		aggregator.mu.Unlock()

		aggregator.logger.Info("ees smf urr report captured",
			zap.String("subscriptionId", sub.ID),
			zap.Uint64("seid", sessRpt.SEID),
			zap.String("ueIp", ueIpv4Addr),
			zap.Uint64("ulBytes", totalUL),
			zap.Uint64("dlBytes", totalDL),
			zap.Uint64("ulPackets", totalULPkt),
			zap.Uint64("dlPackets", totalDLPkt),
			zap.Int("urrCount", reportCount),
			zap.Uint32s("urrIDs", urrIDs),
		)

		// Signal the Run loop that Kernel data is ready.
		// Non-blocking: if the channel already has a signal, skip.
		select {
		case aggregator.kernelReady <- struct{}{}:
		default:
		}
	}
}

// matchesSubscription checks if a session report matches the subscription's target scope.
func (aggregator *Aggregator) matchesSubscription(sub *Subscription, ueIpv4Addr string) bool {
	// Check granularity - only PER_SESSION is fully supported
	if sub.Granularity != GranularityPerSession {
		aggregator.logger.Warn("ees granularity not supported, skipping",
			zap.String("subscriptionId", sub.ID),
			zap.String("granularity", string(sub.Granularity)),
		)
		return false
	}

	// Match target scope
	if sub.Target.AnyUE {
		return true
	}
	if sub.Target.UeIPAddress != "" && sub.Target.UeIPAddress == ueIpv4Addr {
		return true
	}
	return false
}

// computeThroughputIfPossible computes UL/DL bps and pps from bytes/packets and (EndTime - StartTime).
// Guards against non-positive duration by skipping throughput calculation.
func computeThroughputIfPossible(usage *UsageMeasures) {
	durationSeconds := usage.EndTime.Sub(usage.StartTime).Seconds()
	if durationSeconds <= 0 {
		return
	}
	// Bit rate: bytes * 8 / seconds
	usage.ULThroughputBps = (float64(usage.ULBytesDelta) * 8.0) / durationSeconds
	usage.DLThroughputBps = (float64(usage.DLBytesDelta) * 8.0) / durationSeconds

	// Packet rate: packets / seconds
	usage.ULPacketThroughputPps = float64(usage.ULPacketsDelta) / durationSeconds
	usage.DLPacketThroughputPps = float64(usage.DLPacketsDelta) / durationSeconds
}

// AdjustReportPeriod dynamically adjusts the aggregator's report period.
// This should be called once when the first session is established.
// Returns true if adjustment was performed, false if already adjusted or invalid.
func (aggregator *Aggregator) AdjustReportPeriod(urrPeriod time.Duration) bool {
	aggregator.tickerMu.Lock()
	defer aggregator.tickerMu.Unlock()

	// Only adjust once
	if aggregator.periodAdjusted {
		aggregator.logger.Debug("ees period already adjusted, skipping")
		return false
	}

	if urrPeriod <= 0 {
		aggregator.logger.Warn("ees invalid URR period for adjustment",
			zap.Duration("urrPeriod", urrPeriod))
		return false
	}

	urrPeriodSec := int(urrPeriod.Seconds())
	currentPeriodSec := int(aggregator.reportPeriod.Seconds())

	// Calculate the optimal aggregator period (smallest multiple of URR period)
	var newPeriodSec int
	if currentPeriodSec < urrPeriodSec {
		// Current period too short, use URR period
		newPeriodSec = urrPeriodSec
	} else if currentPeriodSec%urrPeriodSec != 0 {
		// Not a multiple, round up to nearest multiple
		multiplier := (currentPeriodSec / urrPeriodSec) + 1
		newPeriodSec = multiplier * urrPeriodSec
	} else {
		// Already a valid multiple, no adjustment needed
		aggregator.periodAdjusted = true
		aggregator.logger.Info("ees period already optimal",
			zap.Int("currentPeriod", currentPeriodSec),
			zap.Int("urrPeriod", urrPeriodSec),
		)
		return false
	}

	newPeriod := time.Duration(newPeriodSec) * time.Second

	// Update period (ticker will be recreated by Run loop)
	aggregator.reportPeriod = newPeriod
	aggregator.periodAdjusted = true

	aggregator.logger.Info("ees aggregator period adjusted",
		zap.Int("oldPeriod", currentPeriodSec),
		zap.Int("newPeriod", newPeriodSec),
		zap.Int("urrPeriod", urrPeriodSec),
	)

	// Signal ticker reset (non-blocking)
	select {
	case aggregator.tickerReset <- struct{}{}:
		aggregator.logger.Debug("ees ticker reset signal sent")
	default:
		aggregator.logger.Debug("ees ticker reset signal already pending")
	}

	return true
}

// DebugDumpBufferState logs a snapshot of the current reportBuffer for debugging.
// Called by PseudoDriver at transition points to understand what data is queued.
func (aggregator *Aggregator) DebugDumpBufferState(label string) {
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	if len(aggregator.reportBuffer) == 0 {
		aggregator.logger.Info("ees DebugDumpBufferState: buffer is empty",
			zap.String("label", label),
		)
		return
	}

	for subID, measures := range aggregator.reportBuffer {
		for i, m := range measures {
			aggregator.logger.Info("ees DebugDumpBufferState: entry",
				zap.String("label", label),
				zap.String("subscriptionId", subID),
				zap.Int("index", i),
				zap.Time("startTime", m.StartTime),
				zap.Time("endTime", m.EndTime),
				zap.String("ueIp", m.UeIpv4Addr),
				zap.Uint64("ulBytes", m.ULBytesDelta),
				zap.Uint64("dlBytes", m.DLBytesDelta),
			)
		}
	}
}
