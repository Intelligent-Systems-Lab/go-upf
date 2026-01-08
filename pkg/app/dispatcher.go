package app

import (
	"github.com/free5gc/go-upf/internal/report"
)

// Dispatcher implements report.Handler and multicasts reports to registered handlers.
// It allows both PFCP (SMF) and EES to receive usage reports.
type Dispatcher struct {
	// Primary: PFCP Server (also handles buffering)
	pfcpHandler report.Handler
	// Secondary: EES Handler (optional, only receives reports)
	eesHandler report.Handler
}

// NewDispatcher creates a dispatcher.
func NewDispatcher(pfcpHandler report.Handler) *Dispatcher {
	return &Dispatcher{
		pfcpHandler: pfcpHandler,
	}
}

// RegisterEESHandler registers the EES handler.
func (d *Dispatcher) RegisterEESHandler(handler report.Handler) {
	d.eesHandler = handler
}

// NotifySessReport multicasts the report to all registered handlers based on URR ID range.
func (d *Dispatcher) NotifySessReport(sessRpt report.SessReport) {
	var pfcpReports []report.Report
	var eesReports []report.Report

	for _, r := range sessRpt.Reports {
		// Identify if this report is for EES (Shadow URR) or SMF (Standard URR)
		isEES := false
		if r.Type() == report.USAR {
			if usar, ok := r.(report.USAReport); ok {
				// Check against Shadow Range (Hardcoded or imported? Imported is better)
				// Using ees.ShadowUrrMin/Max requires import.
				// Let's assume range 20000+ for now as per requirement.
				if usar.URRID >= 20000 {
					isEES = true
				}
			}
		}

		if isEES {
			eesReports = append(eesReports, r)
		} else {
			pfcpReports = append(pfcpReports, r)
		}
	}

	// Dispatch to PFCP (N4)
	if len(pfcpReports) > 0 && d.pfcpHandler != nil {
		d.pfcpHandler.NotifySessReport(report.SessReport{
			SEID:    sessRpt.SEID,
			Reports: pfcpReports,
		})
	}

	// Dispatch to EES
	if len(eesReports) > 0 && d.eesHandler != nil {
		d.eesHandler.NotifySessReport(report.SessReport{
			SEID:    sessRpt.SEID,
			Reports: eesReports,
		})
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
