package ees

import (
	"time"

	"github.com/wmnsk/go-pfcp/ie"
)

// SessURRProvisioner defines the interface for provisioning URRs through a PFCP Session.
// This routes URR creation through the proper PFCP session context, ensuring
// the session's URRIDs map is updated and the driver is called correctly.
type SessURRProvisioner interface {
	// Sess returns the session for the given local SEID.
	Sess(lSeid uint64) (SessContext, error)
}

// SessContext represents a PFCP session context for URR operations.
// This interface is implemented by *pfcp.Sess.
type SessContext interface {
	PFCPSess // Embed PFCPSess from adapter.go
}

// Provisioner handles the construction of PFCP rules (URR) for EES.
// It uses the PFCP session context to properly create and manage URRs.
type Provisioner struct {
	sessProvider SessURRProvisioner
}

// NewProvisioner creates a Provisioner that uses session context for URR operations.
func NewProvisioner(sessProvider SessURRProvisioner) *Provisioner {
	return &Provisioner{
		sessProvider: sessProvider,
	}
}

// PushURRToKernel creates a URR for the given SEID using the provided configuration.
// This routes through the PFCP Session to ensure proper URR registration.
func (p *Provisioner) PushURRToKernel(seid uint64, urrId uint32, event EventType, mode Mode, periodSec int) error {
	// 1. Get the session context
	sess, err := p.sessProvider.Sess(seid)
	if err != nil {
		return err
	}

	// 2. Build Measurement Method IE
	// MEASURES: Volume (Bit 1 - VOLUM)
	// TRENDS: Volume (Bit 1) + Duration (Bit 0 - DURAT)
	vol := 1
	dur := 0
	evt := 0 // Event based not supported yet
	if event == EventUserDataUsageTrends {
		dur = 1
	}
	methodIE := ie.NewMeasurementMethod(dur, vol, evt)

	// 3. Build Reporting Triggers IE
	// PERIODIC: Bit 0 (value 1)
	// Use 3 bytes for proper encoding as per TS 29.244
	trigOctet1 := uint8(0)
	if mode == ModePeriodic {
		trigOctet1 |= 0x01 // PERIO bit
	}
	triggerIE := ie.NewReportingTriggers(trigOctet1, 0, 0)

	// 4. Build Measurement Period IE (if Periodic)
	var periodIE *ie.IE
	if mode == ModePeriodic && periodSec > 0 {
		periodDuration := time.Duration(periodSec) * time.Second
		periodIE = ie.NewMeasurementPeriod(periodDuration)
	}

	// 5. Build Measurement Information IE
	// MNOP (Measurement of Number of Packets): Bit 4 (0x10)
	// This enables packet count reporting in addition to volume
	measurementInfoIE := ie.NewMeasurementInformation(0x10) // MNOP flag

	// 6. Construct CreateURR IE
	ies := []*ie.IE{
		ie.NewURRID(urrId),
		methodIE,
		triggerIE,
		measurementInfoIE,
	}
	if periodIE != nil {
		ies = append(ies, periodIE)
	}

	createUrrIE := ie.NewCreateURR(ies...)

	// 7. Call session's CreateURR (this updates session.URRIDs and calls driver)
	return sess.CreateURR(createUrrIE)
}

// RemoveURRFromKernel removes the Shadow URR via the session context.
func (p *Provisioner) RemoveURRFromKernel(seid uint64, urrId uint32) error {
	sess, err := p.sessProvider.Sess(seid)
	if err != nil {
		return err
	}

	removeUrrIE := ie.NewRemoveURR(ie.NewURRID(urrId))
	_, err = sess.RemoveURR(removeUrrIE)
	return err
}

// UpdatePDRToKernel updates a PDR to bind URR IDs via the session context.
func (p *Provisioner) UpdatePDRToKernel(seid uint64, pdrIE *ie.IE) error {
	sess, err := p.sessProvider.Sess(seid)
	if err != nil {
		return err
	}

	_, err = sess.UpdatePDR(pdrIE)
	return err
}
