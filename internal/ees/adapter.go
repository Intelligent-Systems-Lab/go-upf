package ees

import (
	"github.com/free5gc/go-upf/internal/report"
	"github.com/wmnsk/go-pfcp/ie"
)

// PFCPSess is the interface for PFCP session operations.
// This is implemented by *pfcp.Sess.
type PFCPSess interface {
	CreateURR(req *ie.IE) error
	UpdatePDR(req *ie.IE) ([]report.USAReport, error)
	RemoveURR(req *ie.IE) ([]report.USAReport, error)
}

// PFCPLocalNodeRaw represents the raw LocalNode type from pfcp package.
// We use interface{} pattern to accept *pfcp.LocalNode without import cycle.
type PFCPLocalNodeRaw interface {
	GetSessionContexts() map[uint64]SessionContext
}

// SessGetter is a function that retrieves a session by SEID.
// This allows us to wrap the pfcp.LocalNode.Sess() method without type conflicts.
type SessGetter func(lSeid uint64) (PFCPSess, error)

// LocalNodeAdapter wraps a PFCP LocalNode to implement SessURRProvisioner.
type LocalNodeAdapter struct {
	sessGetter      SessGetter
	sessionProvider PFCPLocalNodeRaw
}

// NewLocalNodeAdapter creates an adapter using a session getter function.
// Usage: ees.NewLocalNodeAdapter(localNode.Sess, localNode)
func NewLocalNodeAdapter(sessGetter SessGetter, rawNode PFCPLocalNodeRaw) *LocalNodeAdapter {
	return &LocalNodeAdapter{
		sessGetter:      sessGetter,
		sessionProvider: rawNode,
	}
}

// Sess implements SessURRProvisioner.
func (a *LocalNodeAdapter) Sess(lSeid uint64) (SessContext, error) {
	return a.sessGetter(lSeid)
}

// GetSessionContexts delegates to the underlying LocalNode.
func (a *LocalNodeAdapter) GetSessionContexts() map[uint64]SessionContext {
	return a.sessionProvider.GetSessionContexts()
}
