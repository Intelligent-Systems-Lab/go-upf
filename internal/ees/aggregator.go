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
	currentSnapshot, err := aggregator.sourceProvider.SnapshotNow()
	if err != nil {
		aggregator.logger.Warn("ees snapshot failed", zap.Error(err))
		return 0, err
	}

	currentKeysSet := make(map[SessionKey]struct{}, len(currentSnapshot))
	for sessionKey := range currentSnapshot {
		currentKeysSet[sessionKey] = struct{}{}
	}

	totalNotifications := 0
	now := time.Now()

	for _, subscription := range aggregator.subscriptionStore.AllSubscriptions() {
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

		usageMeasuresList := computeUsageMeasuresFromCurrent(currentSnapshot)

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
			// Refresh snapshots and switch to periodic after on-demand delivery.
			refreshSnapshots(subscription, currentSnapshot)
			cleanupUnusedSessionKeys(subscription, currentKeysSet)
			subscription.Mode = ModePeriodic
			continue
		}

		// PERIODIC mode: deliver every tick (simple MVP behavior).
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

		// Keep snapshots in sync for potential future use and for cleanup symmetry.
		refreshSnapshots(subscription, currentSnapshot)
		cleanupUnusedSessionKeys(subscription, currentKeysSet)
	}

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
