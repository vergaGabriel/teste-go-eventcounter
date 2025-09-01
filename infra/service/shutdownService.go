package service

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

type ShutdownService struct {
	Timeout time.Duration
}

// Inicia monitoramento de canais e dispara shutdown após timeout
func (s *ShutdownService) MonitorAndShutdown(ctx context.Context, cancelFunc context.CancelFunc,
	counter *CounterService, wg *sync.WaitGroup,
	createdCh, updatedCh, deletedCh <-chan string) {

	idleTimer := time.NewTimer(s.Timeout)

	resetTimer := func() {
		if !idleTimer.Stop() {
			<-idleTimer.C
		}
		idleTimer.Reset(s.Timeout)
	}

	// Goroutine que monitora canais para resetar timer
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-createdCh:
				if ok {
					resetTimer()
				}
			case _, ok := <-updatedCh:
				if ok {
					resetTimer()
				}
			case _, ok := <-deletedCh:
				if ok {
					resetTimer()
				}
			}
		}
	}()

	// Goroutine que aguarda timeout para encerrar
	go func() {
		select {
		case <-idleTimer.C:
			log.Println("Timeout atingido. Iniciando shutdown...")

			// Cancela contexto principal
			cancelFunc()

			// Espera goroutines terminarem
			wg.Wait()

			// Escreve JSONs
			s.ExportJSON(counter)
		case <-ctx.Done():
			return
		}
	}()
}

// Exporta contadores para arquivos JSON por tipo de evento
func (s *ShutdownService) ExportJSON(counter *CounterService) {
	data := counter.GetData()
	for eventType, users := range data {
		filename := eventType + ".json"
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
