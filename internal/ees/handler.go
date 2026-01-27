package ees

import (
	"github.com/free5gc/go-upf/internal/report"
	"go.uber.org/zap"
)

// Handler implements report.Handler for the EES module.
// It receives unsolicited usage reports from the kernel (via Dispatcher)
// and forwards them to the Aggregator (or processes them directly).
type Handler struct {
	aggregator *Aggregator
	logger     *zap.Logger
}

func NewHandler(aggregator *Aggregator, logger *zap.Logger) *Handler {
	return &Handler{
		aggregator: aggregator,
		logger:     logger,
	}
}

// NotifySessReport is called when the Forwarder pushes a report (e.g., periodic URR).
func (h *Handler) NotifySessReport(sessRpt report.SessReport) {
	h.logger.Info("EES Handler received report from dispatcher",
		zap.Uint64("seid", sessRpt.SEID),
		zap.Int("reportCount", len(sessRpt.Reports)),
	)
	// Filter: Check if these reports belong to EES (URR ID based or blind forwarding?)
	// For MVP, we pass it to Aggregator.PushReport
	h.aggregator.PushReport(sessRpt)
}

// PopBufPkt is not supported by EES.
func (h *Handler) PopBufPkt(seid uint64, pdrid uint16) ([]byte, bool) {
	return nil, false
}
