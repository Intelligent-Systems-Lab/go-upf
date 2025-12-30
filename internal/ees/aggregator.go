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

	// [新增] 狀態快取：用來儲存上一次的計數器數值，以便計算增量 (Delta)
	// Key: SessionKey, Value: 上一次的 Counters
	lastSnapshot map[SessionKey]Counters
	// [新增] 記錄上一次 Snapshot 發生的時間
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

		// 初始化 map
		lastSnapshot: make(map[SessionKey]Counters),
		// 初始化時間為現在，避免第一次回報的時間區間過大
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
	// 1. 記錄當下時間 (作為本次區間的 EndTime)
	now := time.Now()

	// 2. 主動拉取最新數據 (Active Pull)
	// 使用 sourceProvider 調用
	currentSnapshot, err := aggregator.sourceProvider.SnapshotNow()
	if err != nil {
		aggregator.logger.Warn("ees snapshot failed", zap.Error(err))
		return 0, err
	}

	totalNotifications := 0

	// 3. 遍歷所有訂閱，計算增量並發送通知
	// [修正 3] 直接使用現有的 AllSubscriptions() 方法，不需新增 Range()
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

		// [核心修正] 計算邏輯重寫：使用 LastSnapshot 來計算 Delta
		var usageMeasuresList []UsageMeasures

		for key, currentCounters := range currentSnapshot {
			// TODO: 未來在此加入 target 過濾邏輯 (如: if key.UEIP != sub.Filter.UEIP { continue })

			// 嘗試從歷史紀錄中獲取上一次的數值
			lastCounters, found := aggregator.lastSnapshot[key]

			var deltaUL, deltaDL uint64
			var deltaULPkt, deltaDLPkt uint64
			var startTime time.Time

			if !found {
				// Case A: 新的 Session (第一次看到)
				// 第一次回報總量作為基準
				deltaUL = currentCounters.ULBytes
				deltaDL = currentCounters.DLBytes
				deltaULPkt = currentCounters.ULPackets
				deltaDLPkt = currentCounters.DLPackets

				// StartTime 設為該 Session 在 Kernel 中的建立時間
				startTime = currentCounters.StartTime
			} else {
				// Case B: 舊的 Session (計算差值)

				// 防止 Counter Overflow 或 Kernel 重啟
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

				// [關鍵修正] StartTime 應該是 "上一次 Snapshot 的時間"
				startTime = aggregator.lastSnapshotTime
			}

			// 組裝回報項目
			usage := UsageMeasures{
				Key:            key,
				ULBytesDelta:   deltaUL,
				DLBytesDelta:   deltaDL,
				ULPacketsDelta: deltaULPkt,
				DLPacketsDelta: deltaDLPkt,
				StartTime:      startTime,
				EndTime:        now,
			}

			// 計算吞吐量 (使用新的 computeThroughputIfPossible 邏輯，或直接在此計算)
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
			// OnDemand 處理完後轉為 Periodic (既有邏輯)，或者直接移除
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

	// 4. 更新狀態：將 Current 變為 Last，為下一次 Tick 做準備
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
