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
	"fmt"
	"math"
	"os"
	"sort"
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
}

// NewPseudoDriver constructs a PseudoDriver.
func NewPseudoDriver(parquetDir string, notifier *Notifier, logger *zap.Logger) *PseudoDriver {
	return &PseudoDriver{
		parquetDir: parquetDir,
		notifier:   notifier,
		logger:     logger,
	}
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

	// Ensure chronological order (run001 -> run015)
	sort.Strings(files)

	periodSec := sub.PeriodSec
	if periodSec <= 0 {
		periodSec = 5 // default fallback
	}

	// Calculate total continuous duration across all files beforehand
	pd.logger.Info("pseudo driver: pre-calculating total time duration for timeline shift")
	totalDurationSec := 0.0
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
		totalDurationSec += (maxTS - minTS) + 0.001
	}

	// Rewind the absolute reference time by the total historical duration.
	// This makes the historical timeline end EXACTLY at time.Now()
	// and prevents StartTime/EndTime overlap with upcoming real-time live traffic.
	referenceTime := time.Now().Add(-time.Duration(totalDurationSec * float64(time.Second)))
	globalTimeOffset := 0.0

	pd.logger.Info("pseudo driver: started warm-start sequence with timestamp rewind",
		zap.Float64("totalDurationSec", totalDurationSec),
		zap.Time("shiftedReferenceTime", referenceTime),
	)

	totalSentCount := 0
	totalPacketsMatched := 0

	// We will collect all filtered packets across all files into one large slice
	// to ensure that aggregation time windows don't get split across file boundaries.
	var allHistoricalPackets []parsedPacket

	for fileIdx, fileObj := range files {
		pd.logger.Info("pseudo driver: loading parquet chunk",
			zap.String("subscriptionId", sub.ID),
			zap.String("file", fileObj),
			zap.Int("fileIdx", fileIdx+1),
			zap.Int("totalFiles", len(files)),
		)

		// 2. Read single Parquet file
		packets, readErr := pd.readParquetFile(fileObj)
		if readErr != nil {
			pd.logger.Error("pseudo driver failed to read parquet file, skipping",
				zap.String("subscriptionId", sub.ID),
				zap.String("file", fileObj),
				zap.Error(readErr),
			)
			continue
		}

		if len(packets) == 0 {
			continue
		}

		// 3. Filter packets by subscription target
		filtered := pd.filterBySubscription(packets, sub)
		if len(filtered) == 0 {
			continue
		}
		totalPacketsMatched += len(filtered)

		// Find local time range of this file BEFORE adjusting to global time
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

		// 4. Adjust timestamps to the unified global timeline and append
		for i := range filtered {
			// Normalize to start at 0, then shift by the accumulated global offset
			filtered[i].timestamp = (filtered[i].timestamp - minTS) + globalTimeOffset
			allHistoricalPackets = append(allHistoricalPackets, filtered[i])
		}

		// Update global offset for the next file (+1ms gap)
		globalTimeOffset += (maxTS - minTS) + 0.001
	}

	// 5. Aggregate into continuous windows ENTIRELY across the unified timeline
	// We pass referenceTime so all windows share the same absolute time baseline
	windows := pd.aggregateIntoWindows(allHistoricalPackets, periodSec, referenceTime)

	// 6. Send each window as a notification sequentially
	for _, measures := range windows {
		if notifyErr := pd.notifier.Notify(sub, measures); notifyErr != nil {
			pd.logger.Warn("pseudo driver: notify failed for window",
				zap.String("subscriptionId", sub.ID),
				zap.Error(notifyErr),
			)
		} else {
			totalSentCount++
		}
	}

	// Record the absolute end time of the last historical window.
	// The live Aggregator will clamp StartTime to never go before this,
	// preventing time regression at the handoff point.
	if len(windows) > 0 {
		lastWindow := windows[len(windows)-1]
		// Find the latest EndTime across all UE measures in the last window
		for _, m := range lastWindow {
			if m.EndTime.After(sub.WarmStartEndTime) {
				sub.WarmStartEndTime = m.EndTime
			}
		}
		pd.logger.Info("pseudo driver: warm-start end time recorded",
			zap.Time("warmStartEndTime", sub.WarmStartEndTime),
		)
	}

	// Update LastNotify to avoid immediate live notification overlap
	sub.LastNotify = time.Now()

	pd.logger.Info("pseudo driver: full warm-start replay complete",
		zap.String("subscriptionId", sub.ID),
		zap.Int("totalWindowsSent", totalSentCount),
		zap.Int("totalPacketsMatched", totalPacketsMatched),
		zap.Float64("totalContinuousDurationSec", globalTimeOffset),
	)
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
func (pd *PseudoDriver) aggregateIntoWindows(packets []parsedPacket, periodSec int, referenceTime time.Time) [][]UsageMeasures {
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
	maxWindowIndex := 0

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
		defaultStartT := referenceTime.Add(time.Duration(windowStartOff * float64(time.Second)))
		defaultEndT := referenceTime.Add(time.Duration(windowEndOff * float64(time.Second)))

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
