package ees

import (
	"time"

	"github.com/free5gc/go-upf/internal/report"
	"github.com/wmnsk/go-pfcp/ie"
)

// ForwarderProvisioner defines the interface for installing/removing URRs.
type ForwarderProvisioner interface {
	CreateURR(uint64, *ie.IE) error
	RemoveURR(uint64, *ie.IE) ([]report.USAReport, error)
	UpdatePDR(uint64, *ie.IE) error
}

// Provisioner handles the construction of PFCP rules (URR) for EES.
type Provisioner struct {
	driver ForwarderProvisioner
}

func NewProvisioner(driver ForwarderProvisioner) *Provisioner {
	return &Provisioner{
		driver: driver,
	}
}

// PushURRToKernel creates a URR for the given SEID using the provided configuration.
func (p *Provisioner) PushURRToKernel(seid uint64, urrId uint32, event EventType, mode Mode, periodSec int) error {
	// 1. Measurement Method
	// MEASURES: Volume (Bit 0)
	// TRENDS: Volume (Bit 0) + Duration (Bit 1)
	vol := 1
	dur := 0
	evt := 0 // Event based not supported yet
	if event == EventUserDataUsageTrends {
		dur = 1
	}
	methodIE := ie.NewMeasurementMethod(dur, vol, evt)

	// 2. Reporting Triggers
	// PERIODIC: Bit 0 (value 1)
	// THRESHOLD: Bit 1 (value 2) - Not yet supported in arguments, defaulting to Periodic if mode is Periodic
	var trigVal uint32 = 0
	if mode == ModePeriodic {
		trigVal |= 1 // PERIO
	}
	// TODO: Handle Thresholds

	triggerIE := ie.NewReportingTriggers(uint8(trigVal)) // ie.NewReportingTriggers takes varargs of uint8 octets

	// 3. Measurement Period (if Periodic)
	var periodIE *ie.IE
	if mode == ModePeriodic && periodSec > 0 {
		periodDuration := time.Duration(periodSec) * time.Second
		periodIE = ie.NewMeasurementPeriod(periodDuration)
	}

	// 4. Create URR IE
	ies := []*ie.IE{
		ie.NewURRID(urrId),
		methodIE,
		triggerIE,
	}
	if periodIE != nil {
		ies = append(ies, periodIE)
	}

	createUrrIE := ie.NewCreateURR(ies...)

	// 5. Call Driver
	return p.driver.CreateURR(seid, createUrrIE)
}

// RemoveURRFromKernel removes the Shadow URR.
func (p *Provisioner) RemoveURRFromKernel(seid uint64, urrId uint32) error {
	// Construct Remove URR IE (usually just ID is needed in the PDR/URR context?)
	// Remove URR maps to PFCP Session Deletion or Modification?
	// It's Session Modification Request -> Remove URR IE.
	// ie.NewRemoveURR(ie.NewURRID(urrId))

	modUrrIE := ie.NewRemoveURR(ie.NewURRID(urrId))

	// Driver.RemoveURR expects the Message or IE?
	// internal/forwarder/driver.go: RemoveURR(uint64, *ie.IE)
	// It likely expects the RemoveURR IE structure.

	_, err := p.driver.RemoveURR(seid, modUrrIE)
	return err
}

// UpdatePDRToKernel updates a PDR.
func (p *Provisioner) UpdatePDRToKernel(seid uint64, pdrIE *ie.IE) error {
	return p.driver.UpdatePDR(seid, pdrIE)
}
