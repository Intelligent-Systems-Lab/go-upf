package app

import (
	"fmt"
	"time"

	"github.com/free5gc/go-upf/internal/report"
)

// Dispatcher implements report.Handler and multicasts reports to registered handlers.
// It allows both PFCP (SMF) and EES to receive usage reports.
type Dispatcher struct {
	// Primary: PFCP Server (also handles buffering)
	pfcpHandler report.Handler
	// Secondary: EES Handler (optional, only receives reports)
	eesHandler report.Handler
	// EES Aggregator (optional, for period adjustment callbacks)
	eesAggregator interface {
		AdjustReportPeriod(urrPeriod time.Duration) bool
	}
	// PerioServer interface (for querying URR periods)
	perioServer interface {
		GetAnyURRPeriod(urrid uint32) time.Duration
	}
}

// NewDispatcher creates a dispatcher.
func NewDispatcher(pfcpHandler report.Handler) *Dispatcher {
	return &Dispatcher{
		pfcpHandler: pfcpHandler,
	}
}

// RegisterEESHandler registers the EES handler and aggregator for callbacks.
func (d *Dispatcher) RegisterEESHandler(handler report.Handler, aggregator interface{}) {
	d.eesHandler = handler
	// Try to assert aggregator to get AdjustReportPeriod method
	if agg, ok := aggregator.(interface{ AdjustReportPeriod(time.Duration) bool }); ok {
		d.eesAggregator = agg
	}
}

// NotifySessReport multicasts the report to all registered handlers.
// Pure Push Mode: All USAReports are forwarded to both handlers.
// - PFCP handler: forwards to SMF (N4)
// - EES handler: aggregates for event exposure (filters URRID >= 7 internally)
func (d *Dispatcher) NotifySessReport(sessRpt report.SessReport) {
	// Debug: Log incoming report
	// Using fmt since we don't have a logger here
	fmt.Printf("[Dispatcher] NotifySessReport: SEID=%#x, ReportCount=%d\n", sessRpt.SEID, len(sessRpt.Reports))

	// Dispatch to PFCP (N4) - all reports
	if d.pfcpHandler != nil {
		d.pfcpHandler.NotifySessReport(sessRpt)
	}

	// Dispatch to EES - all reports (EES aggregator filters by URRID >= 7)
	if d.eesHandler != nil {
		fmt.Printf("[Dispatcher] Forwarding to EES handler\n")
		d.eesHandler.NotifySessReport(sessRpt)
	} else {
		fmt.Printf("[Dispatcher] WARNING: eesHandler is nil!\n")
	}
}

// PopBufPkt delegates buffering logic exclusively to the PFCP handler.
// EES does not handle buffering.
func (d *Dispatcher) PopBufPkt(seid uint64, pdrid uint16) ([]byte, bool) {
	if d.pfcpHandler != nil {
		return d.pfcpHandler.PopBufPkt(seid, pdrid)
	}
	return nil, false
}

// SetPerioServer stores the perio server reference for URR period queries.
func (d *Dispatcher) SetPerioServer(perioServer interface{}) {
	if ps, ok := perioServer.(interface{ GetAnyURRPeriod(uint32) time.Duration }); ok {
		d.perioServer = ps
	}
}

// OnSessionEstablished is called when a new session is established.
// It attempts to adjust the EES aggregator period based on the URR 2 period.
func (d *Dispatcher) OnSessionEstablished() {
	if d.eesAggregator == nil || d.perioServer == nil {
		return
	}

	// Query URR 2 period (the URR used for EES periodic reports)
	urrPeriod := d.perioServer.GetAnyURRPeriod(2)
	if urrPeriod > 0 {
		// Attempt to adjust aggregator period
		adjusted := d.eesAggregator.AdjustReportPeriod(urrPeriod)
		if adjusted {
			fmt.Printf("[Dispatcher] EES aggregator period adjusted based on URR 2 period: %v\n", urrPeriod)
		}
	}
}
