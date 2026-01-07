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
	"time"

	"go.uber.org/zap"
)

// Aggregator periodically reads from Source, builds usage measures per subscription,
// and sends notifications via Notifier.
type Aggregator struct {
	sourceProvider    Source
	subscriptionStore *SubscriptionStore
	reportPeriod      time.Duration
	notifier          *Notifier
	logger            *zap.Logger

	// [New] State cache: used to store the last counter values to calculate delta
	// Key: SessionKey, Value: Last Counters
	lastSnapshot map[SessionKey]Counters
	// [New] Record the time when the last Snapshot occurred
	lastSnapshotTime time.Time
}

// NewAggregator constructs an Aggregator.
func NewAggregator(
	sourceProvider Source,
	subscriptionStore *SubscriptionStore,
	reportPeriod time.Duration,
	notifier *Notifier,
	logger *zap.Logger,
) *Aggregator {
	return &Aggregator{
		sourceProvider:    sourceProvider,
		subscriptionStore: subscriptionStore,
		reportPeriod:      reportPeriod,
		notifier:          notifier,
		logger:            logger,

		// Initialize map
		lastSnapshot: make(map[SessionKey]Counters),
		// Initialize time to now to avoid excessively large interval for the first report
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

// TickOnce pulls the current interval snapshot, sends notifications for each subscription,
// refreshes snapshots, and cleans unused session keys.
// Returns number of notifications attempted (sum over all subscriptions) and any error.
func (aggregator *Aggregator) TickOnce(ctx context.Context) (int, error) {
	// 1. Record current time (as EndTime for this interval)
	now := time.Now()

	// 2. Active Pull latest data
	// Call using sourceProvider
	currentSnapshot, err := aggregator.sourceProvider.SnapshotNow()
	if err != nil {
		aggregator.logger.Warn("ees snapshot failed", zap.Error(err))
		return 0, err
	}

	totalNotifications := 0

	// 3. Iterate over all subscriptions, calculate delta and send notifications
	// [Fix 3] Use existing AllSubscriptions() method directly, no need to add Range()
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

		// [Core Fix] Rewrite calculation logic: Use LastSnapshot to calculate Delta
		var usageMeasuresList []UsageMeasures

		for key, currentCounters := range currentSnapshot {
			// TODO: Add target filtering logic here in the future (e.g., if key.UEIP != sub.Filter.UEIP { continue })

			// Try to get the last values from history
			lastCounters, found := aggregator.lastSnapshot[key]

			var deltaUL, deltaDL uint64
			var deltaULPkt, deltaDLPkt uint64
			var startTime time.Time

			if !found {
				// Case A: New Session (seen for the first time)
				// Use total amount as baseline for the first report
				deltaUL = currentCounters.ULBytes
				deltaDL = currentCounters.DLBytes
				deltaULPkt = currentCounters.ULPackets
				deltaDLPkt = currentCounters.DLPackets

				// Set StartTime to the creation time of the Session in Kernel
				startTime = currentCounters.StartTime
			} else {
				// Case B: Old Session (calculate difference)

				// Prevent Counter Overflow or Kernel restart
				if currentCounters.ULBytes >= lastCounters.ULBytes {
					deltaUL = currentCounters.ULBytes - lastCounters.ULBytes
				} else {
					deltaUL = currentCounters.ULBytes
				}

				if currentCounters.DLBytes >= lastCounters.DLBytes {
					deltaDL = currentCounters.DLBytes - lastCounters.DLBytes
				} else {
					deltaDL = currentCounters.DLBytes
				}

				if currentCounters.ULPackets >= lastCounters.ULPackets {
					deltaULPkt = currentCounters.ULPackets - lastCounters.ULPackets
				} else {
					deltaULPkt = currentCounters.ULPackets
				}

				if currentCounters.DLPackets >= lastCounters.DLPackets {
					deltaDLPkt = currentCounters.DLPackets - lastCounters.DLPackets
				} else {
					deltaDLPkt = currentCounters.DLPackets
				}

				// [Critical Fix] StartTime should be "Time of the last Snapshot"
				startTime = aggregator.lastSnapshotTime
			}

			// Assemble report item
			usage := UsageMeasures{
				Key:            key,
				ULBytesDelta:   deltaUL,
				DLBytesDelta:   deltaDL,
				ULPacketsDelta: deltaULPkt,
				DLPacketsDelta: deltaDLPkt,
				StartTime:      startTime,
				EndTime:        now,
			}

			// Calculate throughput (use new computeThroughputIfPossible logic, or calculate directly here)
			durationSeconds := usage.EndTime.Sub(usage.StartTime).Seconds()
			if durationSeconds > 0 {
				usage.ULThroughputBps = (float64(usage.ULBytesDelta) * 8.0) / durationSeconds
				usage.DLThroughputBps = (float64(usage.DLBytesDelta) * 8.0) / durationSeconds
			}

			usageMeasuresList = append(usageMeasuresList, usage)
		}

		if subscription.Mode == ModeOnDemand {
			if len(usageMeasuresList) > 0 {
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
			// Switch to Periodic after OnDemand processing (existing logic), or remove directly
			subscription.Mode = ModePeriodic
			continue
		}

		// PERIODIC mode
		if len(usageMeasuresList) > 0 {
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

	// 4. Update state: Change Current to Last, prepare for the next Tick
	aggregator.lastSnapshot = currentSnapshot
	aggregator.lastSnapshotTime = now

	return totalNotifications, nil
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
