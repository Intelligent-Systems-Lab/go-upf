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

	"github.com/sirupsen/logrus"

	"github.com/free5gc/go-upf/internal/report"
)

// Aggregator accumulates usage reports pushed from the kernel and periodically
// sends notifications to subscribers. This is a pure Push model - no active polling.
// PerioServerInterface provides access to URR period information
type PerioServerInterface interface {
	GetAnyURRPeriod(urrid uint32) time.Duration
}

// SessionProvider provides access to session contexts
type SessionProvider interface {
	GetSessionContexts() map[uint64]SessionContext
	GetSessionContextUEIP(lSeid uint64) (string, bool)
}

type Aggregator struct {
	subscriptionStore *SubscriptionStore
	reportPeriod      time.Duration
	notifier          *Notifier
	logger            *logrus.Entry

	// [Push Mode] Mutex protects reportBuffer
	mu sync.Mutex
	// [Push Mode] Consolidated reports per subscription: Key=SubscriptionID, SubKey=SessionKey
	reportBuffer map[string]map[SessionKey]*UsageMeasures

	sessionProvider SessionProvider      // Added: to lookup UE IP
	perioServer     PerioServerInterface // Added: to query URR periods

	// Dynamic period adjustment
	ticker         *time.Ticker
	tickerMu       sync.Mutex
	tickerReset    chan struct{} // Signal to reset ticker when period changes
	periodAdjusted bool          // Marks if period has been adjusted once
}

// NewAggregator constructs an Aggregator for pure Push mode.
// Reports are accumulated via PushReport() and sent periodically by TickOnce().
func NewAggregator(
	subscriptionStore *SubscriptionStore,
	reportPeriod time.Duration,
	notifier *Notifier,
	logger *logrus.Entry,
	sessionProvider SessionProvider,
	perioServer PerioServerInterface,
) *Aggregator {
	return &Aggregator{
		subscriptionStore: subscriptionStore,
		reportPeriod:      reportPeriod,
		notifier:          notifier,
		logger:            logger,
		sessionProvider:   sessionProvider,
		perioServer:       perioServer,

		// Initialize report buffer for Push mode
		reportBuffer: make(map[string]map[SessionKey]*UsageMeasures),

		// Initialize ticker reset channel
		tickerReset: make(chan struct{}, 1),
	}
}

// Run starts the periodic loop until ctx is done.
// Uses a dual-loop structure to handle ticker replacement when period is adjusted.
func (aggregator *Aggregator) Run(parentContext context.Context) {
	aggregator.logger.Infof("ees aggregator started (reportPeriod: %v)", aggregator.reportPeriod)

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

		aggregator.logger.Debugf("ees ticker initialized (period: %v)", currentPeriod)

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
				if _, err := aggregator.TickOnce(parentContext); err != nil {
					aggregator.logger.Warnf("ees aggregator tick failed: %v", err)
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
	aggregator.mu.Lock()
	bufferedReports := aggregator.reportBuffer
	aggregator.reportBuffer = make(map[string]map[SessionKey]*UsageMeasures)
	aggregator.mu.Unlock()

	aggregator.logger.Infof("ees tick - processing buffer (subscriptionCount: %d, currentTime: %v)",
		len(bufferedReports), now)

	// Iterate over all subscriptions
	subscriptions := aggregator.subscriptionStore.AllSubscriptions()

	for _, subscription := range subscriptions {
		// MVP scope: only USER_DATA_USAGE_MEASURES + perPduSession
		if subscription.Event != EventUserDataUsageMeasures ||
			subscription.Granularity != GranularityPerSession {
			continue
		}

		// Get consolidated reports for this subscription
		sessionMap, hasReports := bufferedReports[subscription.ID]
		if !hasReports || len(sessionMap) == 0 {
			continue
		}

		// Check if enough time has passed since last notification
		if subscription.Mode == ModePeriodic {
			periodDuration := time.Duration(subscription.PeriodSec) * time.Second
			timeSinceLastNotify := now.Sub(subscription.LastNotify)

			if timeSinceLastNotify < (periodDuration - 100*time.Millisecond) {
				// Keep reports by merging back into main buffer
				aggregator.mu.Lock()
				if _, ok := aggregator.reportBuffer[subscription.ID]; !ok {
					aggregator.reportBuffer[subscription.ID] = make(map[SessionKey]*UsageMeasures)
				}
				for _, v := range sessionMap {
					mergeMeasure(aggregator.reportBuffer[subscription.ID], v)
				}
				aggregator.mu.Unlock()
				continue
			}
		}

		// Prepare consolidated list for notification
		consolidatedList := make([]UsageMeasures, 0, len(sessionMap))
		for _, m := range sessionMap {
			computeThroughputIfPossible(m)
			consolidatedList = append(consolidatedList, *m)
		}

		// Perform notification
		if err := aggregator.notifier.Notify(subscription, consolidatedList); err != nil {
			aggregator.logger.WithFields(logrus.Fields{
				"subscriptionId": subscription.ID,
				"items":          len(consolidatedList),
			}).Warnf("ees notification failed (will retry next tick): %v", err)

			// Keep reports for retry in next tick
			aggregator.mu.Lock()
			if _, ok := aggregator.reportBuffer[subscription.ID]; !ok {
				aggregator.reportBuffer[subscription.ID] = make(map[SessionKey]*UsageMeasures)
			}
			for _, v := range consolidatedList {
				vCopy := v
				mergeMeasure(aggregator.reportBuffer[subscription.ID], &vCopy)
			}
			aggregator.mu.Unlock()
		} else {
			aggregator.logger.WithFields(logrus.Fields{
				"subscriptionId": subscription.ID,
				"items":          len(consolidatedList),
			}).Info("ees notification sent successfully")

			// Update last notify time
			subscription.LastNotify = now
			totalNotifications++
		}
	}

	return totalNotifications, nil
}

// mergeMeasure merges a new measure into an existing map of consolidated measures.
func mergeMeasure(m map[SessionKey]*UsageMeasures, newM *UsageMeasures) {
	existing, ok := m[newM.Key]
	if !ok {
		m[newM.Key] = newM
		return
	}

	if newM.StartTime.Before(existing.StartTime) {
		existing.StartTime = newM.StartTime
	}
	if newM.EndTime.After(existing.EndTime) {
		existing.EndTime = newM.EndTime
	}

	existing.ULBytesDelta += newM.ULBytesDelta
	existing.DLBytesDelta += newM.DLBytesDelta
	existing.ULPacketsDelta += newM.ULPacketsDelta
	existing.DLPacketsDelta += newM.DLPacketsDelta

	// UeIpv4Addr should be the same, but update if existing is empty
	if existing.UeIpv4Addr == "" {
		existing.UeIpv4Addr = newM.UeIpv4Addr
	}
}

// consolidateReports merges multiple UsageMeasures for the same session into one.
// [DEPRECATED] consolidated reports are now merged directly into maps.
func consolidateReports(reports []UsageMeasures) []UsageMeasures {
	if len(reports) <= 1 {
		return reports
	}

	consolidated := make(map[SessionKey]*UsageMeasures)

	for _, r := range reports {
		rCopy := r
		mergeMeasure(consolidated, &rCopy)
	}

	result := make([]UsageMeasures, 0, len(consolidated))
	for _, m := range consolidated {
		computeThroughputIfPossible(m)
		result = append(result, *m)
	}

	return result
}

// PushReport handles unsolicited reports (e.g. from Kernel via Handler).
func (aggregator *Aggregator) PushReport(sessRpt report.SessReport) {
	aggregator.logger.Debugf("PushReport called (SEID: %d, ReportCount: %d)",
		sessRpt.SEID, len(sessRpt.Reports))

	var ueIpv4Addr string
	if aggregator.sessionProvider != nil {
		if ip, ok := aggregator.sessionProvider.GetSessionContextUEIP(sessRpt.SEID); ok {
			ueIpv4Addr = ip
		}
	}

	var totalUL, totalDL, totalULPkt, totalDLPkt uint64
	var startTime, endTime time.Time
	reportCount := 0

	for _, r := range sessRpt.Reports {
		if r.Type() != report.USAR {
			continue
		}
		usarep, ok := r.(report.USAReport)
		if !ok {
			continue
		}

		if usarep.URRID != 2 {
			continue
		}

		totalUL += usarep.VolumMeasure.UplinkVolume
		totalDL += usarep.VolumMeasure.DownlinkVolume
		totalULPkt += usarep.VolumMeasure.UplinkPktNum
		totalDLPkt += usarep.VolumMeasure.DownlinkPktNum

		if reportCount == 0 || usarep.StartTime.Before(startTime) {
			startTime = usarep.StartTime
		}
		if reportCount == 0 || usarep.EndTime.After(endTime) {
			endTime = usarep.EndTime
		}

		reportCount++
	}

	if reportCount == 0 {
		return
	}

	m := &UsageMeasures{
		Key:            SessionKey{LocalSEID: sessRpt.SEID},
		ULBytesDelta:   totalUL,
		DLBytesDelta:   totalDL,
		ULPacketsDelta: totalULPkt,
		DLPacketsDelta: totalDLPkt,
		StartTime:      startTime,
		EndTime:        endTime,
		UeIpv4Addr:     ueIpv4Addr,
	}

	subscriptions := aggregator.subscriptionStore.AllSubscriptions()
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()

	for _, sub := range subscriptions {
		if !aggregator.matchesSubscription(sub, ueIpv4Addr) {
			continue
		}

		if _, ok := aggregator.reportBuffer[sub.ID]; !ok {
			aggregator.reportBuffer[sub.ID] = make(map[SessionKey]*UsageMeasures)
		}
		
		// Create a copy for this subscription since mergeMeasure might modify it
		mCopy := *m
		mergeMeasure(aggregator.reportBuffer[sub.ID], &mCopy)

		aggregator.logger.WithFields(logrus.Fields{
			"subscriptionId": sub.ID,
			"seid":           sessRpt.SEID,
			"ueIp":           ueIpv4Addr,
			"ulBytes":        totalUL,
			"dlBytes":        totalDL,
			"urrCount":       reportCount,
		}).Debug("ees smf urr report captured and consolidated")
	}
}

func (aggregator *Aggregator) matchesSubscription(sub *Subscription, ueIpv4Addr string) bool {
	if sub.Granularity != GranularityPerSession {
		aggregator.logger.WithFields(logrus.Fields{
			"subscriptionId": sub.ID,
			"granularity":    string(sub.Granularity),
		}).Warn("ees granularity not supported, skipping")
		return false
	}

	if sub.Target.AnyUE {
		return true
	}
	if sub.Target.UeIPAddress != "" && sub.Target.UeIPAddress == ueIpv4Addr {
		return true
	}
	return false
}

func computeThroughputIfPossible(usage *UsageMeasures) {
	durationSeconds := usage.EndTime.Sub(usage.StartTime).Seconds()
	if durationSeconds <= 0 {
		return
	}
	usage.ULThroughputBps = (float64(usage.ULBytesDelta) * 8.0) / durationSeconds
	usage.DLThroughputBps = (float64(usage.DLBytesDelta) * 8.0) / durationSeconds
	usage.ULPacketThroughputPps = float64(usage.ULPacketsDelta) / durationSeconds
	usage.DLPacketThroughputPps = float64(usage.DLPacketsDelta) / durationSeconds
}

func (aggregator *Aggregator) AdjustReportPeriod(urrPeriod time.Duration) bool {
	aggregator.tickerMu.Lock()
	defer aggregator.tickerMu.Unlock()

	if aggregator.periodAdjusted {
		aggregator.logger.Debug("ees period already adjusted, skipping")
		return false
	}

	if urrPeriod <= 0 {
		aggregator.logger.Warnf("ees invalid URR period for adjustment: %v", urrPeriod)
		return false
	}

	urrPeriodSec := int(urrPeriod.Seconds())
	currentPeriodSec := int(aggregator.reportPeriod.Seconds())

	var newPeriodSec int
	if currentPeriodSec < urrPeriodSec {
		newPeriodSec = urrPeriodSec
	} else if currentPeriodSec%urrPeriodSec != 0 {
		newPeriodSec = ((currentPeriodSec / urrPeriodSec) + 1) * urrPeriodSec
	} else {
		aggregator.periodAdjusted = true
		aggregator.logger.Infof("ees period already optimal (current: %ds, urr: %ds)", currentPeriodSec, urrPeriodSec)
		return false
	}

	newPeriod := time.Duration(newPeriodSec) * time.Second
	aggregator.reportPeriod = newPeriod
	aggregator.periodAdjusted = true

	aggregator.logger.Infof("ees aggregator period adjusted (old: %ds, new: %ds, urr: %ds)",
		currentPeriodSec, newPeriodSec, urrPeriodSec)

	select {
	case aggregator.tickerReset <- struct{}{}:
		aggregator.logger.Debug("ees ticker reset signal sent")
	default:
		aggregator.logger.Debug("ees ticker reset signal already pending")
	}

	return true
}
