package service

import (
	"context"
	"sync"

	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

type CounterService struct {
	mu    sync.Mutex
	count map[string]map[string]int // eventType -> userID -> count
}

func NewCounterService() *CounterService {
	return &CounterService{
		count: make(map[string]map[string]int),
	}
}

// Incrementa contador interno
func (c *CounterService) Increment(eventType eventcounter.EventType, userID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.count[string(eventType)]; !ok {
		c.count[string(eventType)] = make(map[string]int)
	}

	c.count[string(eventType)][userID]++
}

// Implementa a interface eventcounter.Consumer
func (c *CounterService) Created(_ context.Context, userID string) error {
	c.Increment(eventcounter.EventCreated, userID)
	return nil
}

func (c *CounterService) Updated(_ context.Context, userID string) error {
	c.Increment(eventcounter.EventUpdated, userID)
	return nil
}

func (c *CounterService) Deleted(_ context.Context, userID string) error {
	c.Increment(eventcounter.EventDeleted, userID)
	return nil
}

// Retorna os dados para exportação
func (c *CounterService) GetData() map[string]map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Retorna uma cópia
	copy := make(map[string]map[string]int)
	for eventType, users := range c.count {
		copy[eventType] = make(map[string]int)
		for user, count := range users {
			copy[eventType][user] = count
		}
	}
	return copy
}
