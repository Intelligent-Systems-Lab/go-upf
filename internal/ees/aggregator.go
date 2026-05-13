// Package ees - UPF Event Exposure Service (EES)
// aggregator.go: periodic/on-demand reporting loop for USER_DATA_USAGE_MEASURES.

package ees

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/free5gc/go-upf/internal/report"
)

// PerioServerInterface provides access to URR period information
type PerioServerInterface interface {
	GetAnyURRPeriod(urrid uint32) time.Duration
}

type Aggregator struct {
	subscriptionStore *SubscriptionStore
	reportPeriod      time.Duration
	notifier          *Notifier
	logger            *logrus.Entry

	mu           sync.Mutex
	reportBuffer map[string][]UsageMeasures

	sessionProvider SessionProvider
	perioServer     PerioServerInterface
	pseudoDriver    *PseudoDriver

	ticker         *time.Ticker
	tickerMu       sync.Mutex
	tickerReset    chan struct{}
	periodAdjusted bool

	tickDone     chan struct{}
	tickDoneMu   sync.Mutex
	lastTickTime time.Time
	lastTickMu   sync.RWMutex

	firstURRReceived bool
	startMu          sync.Mutex

	kernelReady chan struct{}

	lastSnapshotTime time.Time
}

func NewAggregator(
	subscriptionStore *SubscriptionStore,
	reportPeriod time.Duration,
	notifier *Notifier,
	logger *logrus.Entry,
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
		reportBuffer:      make(map[string][]UsageMeasures),
		tickerReset:       make(chan struct{}, 1),
		kernelReady:       make(chan struct{}, 1),
		tickDone:          make(chan struct{}),
		lastSnapshotTime:  time.Now(),
	}
}

func (aggregator *Aggregator) Run(parentContext context.Context) {
	aggregator.logger.WithField("reportPeriod", aggregator.reportPeriod).Info("ees aggregator started")

	for {
		aggregator.tickerMu.Lock()
		if aggregator.ticker != nil {
			aggregator.ticker.Stop()
		}
		aggregator.ticker = time.NewTicker(aggregator.reportPeriod)
		tickerC := aggregator.ticker.C
		currentPeriod := aggregator.reportPeriod
		aggregator.tickerMu.Unlock()

		aggregator.logger.WithField("period", currentPeriod).Debug("ees ticker initialized")

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
				aggregator.logger.Info("ees ticker reset signal received, recreating ticker")
				break tickerLoop
			case <-tickerC:
				select {
				case <-aggregator.kernelReady:
					time.Sleep(50 * time.Millisecond)
					aggregator.logger.Debug("ees kernel-ready signal received, proceeding to TickOnce")
				case <-time.After(2 * time.Second):
					aggregator.logger.Debug("ees kernel-ready timeout, proceeding to TickOnce")
				}

				aggregator.lastTickMu.Lock()
				nowRaw := time.Now()
				if aggregator.lastTickTime.IsZero() {
					aggregator.lastTickTime = nowRaw.Truncate(time.Second)
				} else {
					aggregator.lastTickTime = aggregator.lastTickTime.Add(currentPeriod).Truncate(time.Second)
				}
				aggregator.lastTickMu.Unlock()

				if _, err := aggregator.TickOnce(parentContext); err != nil {
					aggregator.logger.WithError(err).Warn("ees aggregator tick failed")
				}

				aggregator.tickDoneMu.Lock()
				close(aggregator.tickDone)
				aggregator.tickDone = make(chan struct{})
				aggregator.tickDoneMu.Unlock()
			}
		}
	}
}

func (aggregator *Aggregator) WaitForTick() {
	aggregator.tickDoneMu.Lock()
	ch := aggregator.tickDone
	aggregator.tickDoneMu.Unlock()
	<-ch
}

func (aggregator *Aggregator) GetLastTickTime() time.Time {
	aggregator.lastTickMu.RLock()
	defer aggregator.lastTickMu.RUnlock()
	return aggregator.lastTickTime
}

func (aggregator *Aggregator) TickOnce(ctx context.Context) (int, error) {
	now := time.Now()
	totalNotifications := 0

	aggregator.mu.Lock()
	bufferedReports := aggregator.reportBuffer
	aggregator.reportBuffer = make(map[string][]UsageMeasures)
	newBuffer := make(map[string][]UsageMeasures)
	aggregator.mu.Unlock()

	subscriptions := aggregator.subscriptionStore.AllSubscriptions()

	aggregator.logger.WithFields(logrus.Fields{
		"subscriptionCount": len(subscriptions),
		"currentTime":       now,
	}).Info("ees tick starting")

	for _, subscription := range subscriptions {
		if subscription.Event != EventUserDataUsageMeasures ||
			subscription.Granularity != GranularityPerSession {
			continue
		}

		// FINAL GATE: If Pseudo-driver is still doing IO (Phase 1), skip this subscription
		// to prevent Kernel data from "moving the timeline" prematurely.
		subscription.SimMu.RLock()
		isWarmingUp := subscription.WarmupPending
		subscription.SimMu.RUnlock()
		if isWarmingUp {
			// Re-buffer the data for next tick
			aggregator.mu.Lock()
			if list, ok := bufferedReports[subscription.ID]; ok {
				aggregator.reportBuffer[subscription.ID] = append(aggregator.reportBuffer[subscription.ID], list...)
			}
			aggregator.mu.Unlock()

			aggregator.logger.WithField("subId", subscription.ID).Info("ees aggregator: waiting for pseudo-driver warm-up, skipping tick")
			continue
		}

		subscription.Mu.Lock()
		usageMeasuresList, hasReports := bufferedReports[subscription.ID]

		if !hasReports || len(usageMeasuresList) == 0 {
			subscription.Mu.Unlock()
			continue
		}

		periodDuration := time.Duration(subscription.PeriodSec) * time.Second
		timeSinceLastNotify := now.Sub(subscription.LastNotify)
		const tolerance = 1000 * time.Millisecond

		if subscription.Mode == ModePeriodic {
			if timeSinceLastNotify+tolerance < periodDuration {
				aggregator.mu.Lock()
				newBuffer[subscription.ID] = append(newBuffer[subscription.ID], usageMeasuresList...)
				aggregator.mu.Unlock()
				subscription.Mu.Unlock()
				continue
			}
		}

		// T-1 Lag Implementation: Only process windows that ended at least PeriodSec ago
		lagThreshold := now.Add(-periodDuration)
		var processQueue []UsageMeasures
		var keepQueue []UsageMeasures

		for i := range usageMeasuresList {
			m := usageMeasuresList[i]
			if !subscription.GridAnchor.IsZero() {
				snappedEnd := snapTime(m.EndTime, subscription.GridAnchor, periodDuration)
				m.EndTime = snappedEnd
				m.StartTime = snappedEnd.Add(-periodDuration)
			} else {
				snappedEnd := m.EndTime.Round(periodDuration)
				m.EndTime = snappedEnd
				m.StartTime = snappedEnd.Add(-periodDuration)
			}

			if m.EndTime.Before(lagThreshold) || m.EndTime.Equal(lagThreshold) || subscription.Mode == ModeOnDemand {
				processQueue = append(processQueue, m)
			} else {
				keepQueue = append(keepQueue, m)
			}
		}

		if len(keepQueue) > 0 {
			aggregator.mu.Lock()
			newBuffer[subscription.ID] = append(newBuffer[subscription.ID], keepQueue...)
			aggregator.mu.Unlock()
		}

		if len(processQueue) == 0 {
			subscription.Mu.Unlock()
			continue
		}

		consolidatedList := aggregator.consolidateWithPriority(processQueue)

		var filteredReports []UsageMeasures
		for _, m := range consolidatedList {
			lastSent, ok := subscription.LastSentStartTime[m.UeIpv4Addr]
			if !ok || m.StartTime.After(lastSent) {
				filteredReports = append(filteredReports, m)
			} else {
				aggregator.logger.WithFields(logrus.Fields{
					"subscriptionId": subscription.ID,
					"ueIp":           m.UeIpv4Addr,
					"startTime":      m.StartTime,
					"lastSent":       lastSent,
				}).Info("ees aggregator dropping duplicate window (already sent)")
			}
		}

		if len(filteredReports) == 0 {
			subscription.Mu.Unlock()
			continue
		}

		for i := range filteredReports {
			computeThroughputIfPossible(&filteredReports[i])
		}

		if err := aggregator.notifier.Notify(subscription, filteredReports); err != nil {
			aggregator.logger.WithFields(logrus.Fields{
				"subscriptionId": subscription.ID,
				"error":          err,
			}).Warn("ees notify failed")
		} else {
			totalNotifications++
			subscription.LastNotify = now
			for _, m := range filteredReports {
				if m.StartTime.After(subscription.LastSentStartTime[m.UeIpv4Addr]) {
					subscription.LastSentStartTime[m.UeIpv4Addr] = m.StartTime
				}
			}
			aggregator.logger.WithFields(logrus.Fields{
				"subscriptionId": subscription.ID,
				"count":          len(filteredReports),
			}).Info("ees notification sent successfully")
		}

		if subscription.Mode == ModeOnDemand {
			subscription.Mode = ModePeriodic
		}
		subscription.Mu.Unlock()
	}

	aggregator.mu.Lock()
	for subID, reports := range newBuffer {
		aggregator.reportBuffer[subID] = append(aggregator.reportBuffer[subID], reports...)
	}
	aggregator.mu.Unlock()

	return totalNotifications, nil
}

func (aggregator *Aggregator) consolidateWithPriority(reports []UsageMeasures) []UsageMeasures {
	if len(reports) == 0 {
		return nil
	}
	consolidated := make(map[string]*UsageMeasures)
	for _, r := range reports {
		key := fmt.Sprintf("%s-%d", r.UeIpv4Addr, r.StartTime.Unix())
		existing, ok := consolidated[key]
		if !ok {
			rCopy := r
			consolidated[key] = &rCopy
			continue
		}

		// Additive logic: both Pseudo and Kernel sources contribute to the total
		existing.ULBytesDelta += r.ULBytesDelta
		existing.DLBytesDelta += r.DLBytesDelta
		existing.ULPacketsDelta += r.ULPacketsDelta
		existing.DLPacketsDelta += r.DLPacketsDelta
	}
	result := make([]UsageMeasures, 0, len(consolidated))
	for _, m := range consolidated {
		result = append(result, *m)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].StartTime.Before(result[j].StartTime)
	})
	return result
}

func snapTime(t time.Time, anchor time.Time, period time.Duration) time.Time {
	offset := t.Sub(anchor)
	halfPeriod := period / 2
	var roundedOffset time.Duration
	if offset >= 0 {
		roundedOffset = ((offset + halfPeriod) / period) * period
	} else {
		roundedOffset = ((offset - halfPeriod) / period) * period
	}
	return anchor.Add(roundedOffset).Truncate(time.Second)
}

func (aggregator *Aggregator) PushHistoricalMeasures(sub *Subscription, measures []UsageMeasures, logicalTime time.Time) {
	if len(measures) == 0 {
		return
	}
	for i := range measures {
		measures[i].Source = SourcePseudo
	}
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()
	aggregator.reportBuffer[sub.ID] = append(aggregator.reportBuffer[sub.ID], measures...)
}

func (aggregator *Aggregator) PushLiveMeasures(sub *Subscription, measures []UsageMeasures) {
	if len(measures) == 0 {
		return
	}
	for i := range measures {
		measures[i].Source = SourcePseudo
	}
	aggregator.startMu.Lock()
	if !aggregator.firstURRReceived {
		aggregator.firstURRReceived = true
		aggregator.logger.WithFields(logrus.Fields{
			"subscriptionId": sub.ID, "arrival_time": time.Now(),
		}).Info("ees aggregator received first live URR, forcing Ticker re-sync")
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

func (aggregator *Aggregator) PushReport(sessRpt report.SessReport) {
	var ueIpv4Addr string
	if aggregator.sessionProvider != nil {
		contexts := aggregator.sessionProvider.GetSessionContexts()
		if ctx, ok := contexts[sessRpt.SEID]; ok {
			ueIpv4Addr = ctx.UeIPv4Addr
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
		if !ok || usarep.URRID != 2 {
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
	m := UsageMeasures{
		Key: SessionKey{LocalSEID: sessRpt.SEID}, Source: SourceKernel,
		ULBytesDelta: totalUL, DLBytesDelta: totalDL, ULPacketsDelta: totalULPkt, DLPacketsDelta: totalDLPkt,
		StartTime: startTime, EndTime: endTime, UeIpv4Addr: ueIpv4Addr,
	}
	subscriptions := aggregator.subscriptionStore.AllSubscriptions()
	for _, sub := range subscriptions {
		if !aggregator.matchesSubscription(sub, ueIpv4Addr) {
			continue
		}
		sub.Mu.Lock()
		if aggregator.pseudoDriver != nil {
			// ANCHOR FIX: Use actual network timestamp instead of time.Now()
			aggregator.pseudoDriver.SignalFirstURR(m.EndTime)
		}
		if sub.GridAnchor.IsZero() {
			if aggregator.pseudoDriver == nil {
				sub.GridAnchor = m.EndTime
			} else {
				sub.GridAnchor = m.EndTime.Truncate(time.Second)
			}
		}
		if m.StartTime.Before(sub.GridAnchor.Add(-2 * time.Second)) {
			sub.Mu.Unlock()
			continue
		}
		subMeasure := m
		aggregator.mu.Lock()
		aggregator.reportBuffer[sub.ID] = append(aggregator.reportBuffer[sub.ID], subMeasure)
		aggregator.mu.Unlock()
		select {
		case aggregator.kernelReady <- struct{}{}:
		default:
		}
		sub.Mu.Unlock()
	}
}

func (aggregator *Aggregator) matchesSubscription(sub *Subscription, ueIpv4Addr string) bool {
	if sub.Granularity != GranularityPerSession {
		return false
	}
	if sub.Target.AnyUE {
		return true
	}
	return sub.Target.UeIPAddress != "" && sub.Target.UeIPAddress == ueIpv4Addr
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
		return false
	}
	if urrPeriod <= 0 {
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
		return false
	}
	aggregator.reportPeriod = time.Duration(newPeriodSec) * time.Second
	aggregator.periodAdjusted = true
	select {
	case aggregator.tickerReset <- struct{}{}:
	default:
	}
	return true
}

func (aggregator *Aggregator) DebugDumpBufferState(label string) {
	aggregator.mu.Lock()
	defer aggregator.mu.Unlock()
	for subID, measures := range aggregator.reportBuffer {
		for i, m := range measures {
			aggregator.logger.WithFields(logrus.Fields{
				"label": label, "subscriptionId": subID, "index": i, "source": m.Source, "ueIp": m.UeIpv4Addr, "startTime": m.StartTime,
			}).Info("ees DebugDumpBufferState: entry")
		}
	}
}
