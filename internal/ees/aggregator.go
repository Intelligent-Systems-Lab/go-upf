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
	"sync"
	"time"

	"github.com/free5gc/go-upf/internal/report"
	"go.uber.org/zap"
)

// Aggregator accumulates usage reports pushed from the kernel and periodically
// sends notifications to subscribers. This is a pure Push model - no active polling.
type Aggregator struct {
	subscriptionStore *SubscriptionStore
	reportPeriod      time.Duration
	notifier          *Notifier
	logger            *zap.Logger

	// [Push Mode] Mutex protects reportBuffer
	mu sync.Mutex
	// [Push Mode] Accumulated reports per subscription: Key=SubscriptionID
	reportBuffer map[string][]UsageMeasures

	sessionProvider SessionProvider // Added: to lookup UE IP

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
) *Aggregator {
	return &Aggregator{
		subscriptionStore: subscriptionStore,
		reportPeriod:      reportPeriod,
		notifier:          notifier,
		logger:            logger,
		sessionProvider:   sessionProvider,

		// Initialize report buffer for Push mode
		reportBuffer: make(map[string][]UsageMeasures),

		// Initialize legacy snapshot maps (kept for potential future use)
		lastSnapshot:     make(map[SessionKey]Counters),
		lastSnapshotTime: time.Now(),
	}
}

// Run starts the periodic loop until ctx is done.
func (aggregator *Aggregator) Run(parentContext context.Context) {

	ticker := time.NewTicker(aggregator.reportPeriod)
	defer ticker.Stop()

	aggregator.logger.Info("ees aggregator started",
		zap.Duration("reportPeriod", aggregator.reportPeriod),
	)

	for {
		select {
		case <-parentContext.Done():
			aggregator.logger.Info("ees aggregator stopped")
			return
		case <-ticker.C:
			if _, err := aggregator.TickOnce(parentContext); err != nil {
				aggregator.logger.Warn("ees aggregator tick failed", zap.Error(err))
			}
		}
	}
}

// TickOnce sends notifications using accumulated reports from the Push buffer.
// This is the Pure Push model - no active polling.
// Returns number of notifications attempted (sum over all subscriptions) and any error.
func (aggregator *Aggregator) TickOnce(ctx context.Context) (int, error) {
	_ = ctx // ctx reserved for future cancellation support

	now := time.Now()
	totalNotifications := 0

	// Lock and swap out the buffer
	aggregator.mu.Lock()
	bufferedReports := aggregator.reportBuffer
	aggregator.reportBuffer = make(map[string][]UsageMeasures)
	aggregator.mu.Unlock()

	// Iterate over all subscriptions
	subscriptions := aggregator.subscriptionStore.AllSubscriptions()

	for _, subscription := range subscriptions {
		// MVP scope: only USER_DATA_USAGE_MEASURES + perPduSession
		if subscription.Event != EventUserDataUsageMeasures ||
			subscription.Granularity != GranularityPerPduSession {
			aggregator.logger.Debug("ees aggregator skip unsupported subscription",
				zap.String("subscriptionId", subscription.ID),
				zap.String("event", string(subscription.Event)),
				zap.String("granularity", string(subscription.Granularity)),
			)
			continue
		}

		// Get accumulated reports for this subscription
		usageMeasuresList, hasReports := bufferedReports[subscription.ID]

		if subscription.Mode == ModeOnDemand {
			if hasReports && len(usageMeasuresList) > 0 {
				if err := aggregator.notifier.Notify(subscription, usageMeasuresList); err != nil {
					aggregator.logger.Warn("ees notify (on-demand) failed",
						zap.String("subscriptionId", subscription.ID),
						zap.Error(err),
						zap.Int("items", len(usageMeasuresList)),
					)
				} else {
					totalNotifications++
					subscription.LastNotify = now
					aggregator.logger.Debug("ees notify (on-demand) success",
						zap.String("subscriptionId", subscription.ID),
						zap.Int("items", len(usageMeasuresList)),
					)
				}
			}
			// Switch to Periodic after OnDemand processing
			subscription.Mode = ModePeriodic
			continue
		}

		// PERIODIC mode
		if hasReports && len(usageMeasuresList) > 0 {
			if err := aggregator.notifier.Notify(subscription, usageMeasuresList); err != nil {
				aggregator.logger.Warn("ees notify (periodic) failed",
					zap.String("subscriptionId", subscription.ID),
					zap.Error(err),
					zap.Int("items", len(usageMeasuresList)),
				)
			} else {
				totalNotifications++
				subscription.LastNotify = now
				aggregator.logger.Debug("ees notify (periodic) success",
					zap.String("subscriptionId", subscription.ID),
					zap.Int("items", len(usageMeasuresList)),
				)
			}
		}
	}

	aggregator.lastSnapshotTime = now
	return totalNotifications, nil
}

// PushReport handles unsolicited reports (e.g. from Kernel via Handler).
// Reports are accumulated in reportBuffer and sent during the next TickOnce().
func (aggregator *Aggregator) PushReport(sessRpt report.SessReport) {
	for _, r := range sessRpt.Reports {
		if r.Type() != report.USAR {
			continue
		}
		usarep, ok := r.(report.USAReport)
		if !ok {
			continue
		}

		// Iterate subscriptions to see if this report is relevant
		subscriptions := aggregator.subscriptionStore.AllSubscriptions()
		for _, sub := range subscriptions {
			// Check Shadow URR ID match
			if sub.ShadowURRID != usarep.URRID {
				continue
			}

			// Build UsageMeasure
			m := UsageMeasures{
				Key: SessionKey{LocalSEID: sessRpt.SEID},
				// Note: RemoteSEID is unknown here without lookup.
				ULBytesDelta:   usarep.VolumMeasure.UplinkVolume,
				DLBytesDelta:   usarep.VolumMeasure.DownlinkVolume,
				ULPacketsDelta: usarep.VolumMeasure.UplinkPktNum,
				DLPacketsDelta: usarep.VolumMeasure.DownlinkPktNum,
				StartTime:      usarep.StartTime,
				EndTime:        usarep.EndTime,
			}

			// Populate UE IP from SessionContext and log source PDRs
			var contributingPDRs []uint16
			if aggregator.sessionProvider != nil {
				contexts := aggregator.sessionProvider.GetSessionContexts()
				if ctx, ok := contexts[sessRpt.SEID]; ok {
					m.UeIpv4Addr = ctx.UeIPv4Addr

					// Find PDRs associated with this URR
					for _, pdr := range ctx.PDRs {
						for _, uid := range pdr.URRIDs {
							if uid == usarep.URRID {
								contributingPDRs = append(contributingPDRs, pdr.PDRID)
								break
							}
						}
					}
				}
			}

			// Compute Throughput
			computeThroughputIfPossible(&m)

			// Accumulate to buffer (will be sent in TickOnce)
			aggregator.mu.Lock()
			aggregator.reportBuffer[sub.ID] = append(aggregator.reportBuffer[sub.ID], m)
			aggregator.mu.Unlock()

			aggregator.logger.Info("ees shadow urr report received",
				zap.String("subscriptionId", sub.ID),
				zap.Uint32("shadowURRId", sub.ShadowURRID), // Log Shadow ID
				zap.Uint64("seid", sessRpt.SEID),
				zap.Uint64("ulBytes", m.ULBytesDelta),
				zap.Uint64("dlBytes", m.DLBytesDelta),
				zap.Uint16s("sourcePDRs", contributingPDRs),
			)
		}
	}
}

// computeUsageMeasuresFromCurrent uses the provider's current counters as-is (interval semantics)
// and derives throughputs from StartTime/EndTime.
func computeUsageMeasuresFromCurrent(currentSnapshot map[SessionKey]Counters) []UsageMeasures {
	usageMeasuresList := make([]UsageMeasures, 0, len(currentSnapshot))
	for sessionKey, currentCounters := range currentSnapshot {
		usage := UsageMeasures{
			Key:            sessionKey,
			ULBytesDelta:   currentCounters.ULBytes,
			DLBytesDelta:   currentCounters.DLBytes,
			ULPacketsDelta: currentCounters.ULPackets,
			DLPacketsDelta: currentCounters.DLPackets,
			StartTime:      currentCounters.StartTime,
			EndTime:        currentCounters.EndTime,
		}
		computeThroughputIfPossible(&usage)
		usageMeasuresList = append(usageMeasuresList, usage)
	}
	return usageMeasuresList
}

// computeThroughputIfPossible computes UL/DL bps from bytes and (EndTime - StartTime).
// Guards against non-positive duration by skipping throughput calculation.
func computeThroughputIfPossible(usage *UsageMeasures) {
	durationSeconds := usage.EndTime.Sub(usage.StartTime).Seconds()
	if durationSeconds <= 0 {
		return
	}
	usage.ULThroughputBps = (float64(usage.ULBytesDelta) * 8.0) / durationSeconds
	usage.DLThroughputBps = (float64(usage.DLBytesDelta) * 8.0) / durationSeconds
}

// refreshSnapshots copies the current snapshot into the subscription.Snapshots map.
// While interval semantics do not need previous snapshots for delta, we keep this
// for cleanup symmetry and potential future cumulative modes.
func refreshSnapshots(subscription *Subscription, currentSnapshot map[SessionKey]Counters) {
	if subscription.Snapshots == nil {
		subscription.Snapshots = make(map[SessionKey]Counters, len(currentSnapshot))
	}
	for sessionKey, currentCounters := range currentSnapshot {
		subscription.Snapshots[sessionKey] = currentCounters
	}
}

// cleanupUnusedSessionKeys removes keys from subscription.Snapshots that are not present
// in the current snapshot key set (simple "clean unused source" strategy).
func cleanupUnusedSessionKeys(subscription *Subscription, currentKeysSet map[SessionKey]struct{}) {
	for sessionKey := range subscription.Snapshots {
		if _, stillPresent := currentKeysSet[sessionKey]; !stillPresent {
			delete(subscription.Snapshots, sessionKey)
		}
	}
}
