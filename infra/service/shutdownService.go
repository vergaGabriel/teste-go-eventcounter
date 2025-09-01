package service

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

type ShutdownService struct {
	Timeout time.Duration
	lastMessage time.Time
	mu sync.Mutex
}

// Atualiza o horário da última mensagem recebida
func (s *ShutdownService) UpdateLastMessage() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastMessage = time.Now()
}

// Inicia monitoramento de inatividade e dispara shutdown após timeout
func (s *ShutdownService) MonitorAndShutdown(ctx context.Context, cancelFunc context.CancelFunc, counter *CounterService) {
	s.UpdateLastMessage() // inicializa o timer

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				elapsed := time.Since(s.lastMessage)
				s.mu.Unlock()
				if elapsed >= s.Timeout {
					log.Println("Timeout atingido. Iniciando shutdown...")
					cancelFunc()
					s.ExportJSON(counter)
					return
				}
			}
		}
	}()
}

// Exporta contadores para arquivos JSON com nomes baseados nos tipos de evento
func (s *ShutdownService) ExportJSON(counter *CounterService) {
	data := counter.GetData()
	eventTypes := []eventcounter.EventType{
		eventcounter.EventCreated,
		eventcounter.EventUpdated,
		eventcounter.EventDeleted,
	}

	for _, eventType := range eventTypes {
		users, ok := data[string(eventType)]
		if !ok {
			continue
		}
		filename := string(eventType) + ".json"
		file, err := os.Create(filename)
		if err != nil {
			log.Printf("Erro ao criar arquivo %s: %v", filename, err)
			continue
		}
		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")
		if err := enc.Encode(users); err != nil {
			log.Printf("Erro ao escrever JSON %s: %v", filename, err)
		}
		file.Close()
	}
	log.Println("JSONs exportados com sucesso.")
}
