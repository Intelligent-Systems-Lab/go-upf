package ees

import (
	"errors"
	"sync"
)

const (
	ShadowUrrMin = 20000
	ShadowUrrMax = 100000
)

type IDManager struct {
	mu     sync.Mutex
	used   map[uint32]bool
	nextID uint32
}

func NewIDManager() *IDManager {
	return &IDManager{
		used:   make(map[uint32]bool),
		nextID: ShadowUrrMin,
	}
}

// Allocate returns a unique URR ID for Shadow URR usage.
func (m *IDManager) Allocate() (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	start := m.nextID
	for {
		if !m.used[m.nextID] {
			id := m.nextID
			m.used[id] = true
			m.nextID++
			if m.nextID > ShadowUrrMax {
				m.nextID = ShadowUrrMin
			}
			return id, nil
		}
		m.nextID++
		if m.nextID > ShadowUrrMax {
			m.nextID = ShadowUrrMin
		}
		if m.nextID == start {
			return 0, errors.New("no shadow URR IDs available")
		}
	}
}

// Release returns the ID to the pool.
func (m *IDManager) Release(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.used, id)
}

// IsShadowURR checks if an ID is within the shadow range.
func (m *IDManager) IsShadowURR(id uint32) bool {
	return id >= ShadowUrrMin && id <= ShadowUrrMax
}
