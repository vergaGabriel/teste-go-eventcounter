package service

import (
	"sync"
)

// CounterService gerencia a contagem por tipo de evento e usuário
type CounterService struct {
	mu   sync.Mutex
	data map[string]map[string]int // data[eventType][userID] = contador
}

// Novo serviço de contador
func NewCounterService() *CounterService {
	return &CounterService{
		data: make(map[string]map[string]int),
	}
}

// Incrementa o contador para um usuário e tipo de evento
func (s *CounterService) Increment(eventType, userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[eventType]; !ok {
		s.data[eventType] = make(map[string]int)
	}
	s.data[eventType][userID]++
}

// Retorna os dados atuais do contador
func (s *CounterService) GetData() map[string]map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Retorna uma cópia para evitar race conditions
	copyData := make(map[string]map[string]int)
	for eventType, users := range s.data {
		copyData[eventType] = make(map[string]int)
		for user, count := range users {
			copyData[eventType][user] = count
		}
	}
	return copyData
}
