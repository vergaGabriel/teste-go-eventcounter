package main

import (
	"context"
	"log"
	"time"

	rabbit "github.com/reb-felipe/eventcounter/infra/consumer"
	service "github.com/reb-felipe/eventcounter/infra/service"
)

func main() {
	// Contexto principal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Inicializa o CounterService (responsável pela contagem)
	counterService := service.NewCounterService()

	// Inicializa o ShutdownService com timeout de 5 segundos
	shutdownService := &service.ShutdownService{Timeout: 5 * time.Second}

	// Inicializa o consumer do RabbitMQ, passando o CounterService como Consumer
	rabbitURL := "amqp://guest:guest@localhost:5672/" // ou variável de ambiente
	consumer, err := rabbit.ConnectRabbit(rabbitURL, counterService)
	if err != nil {
		log.Fatalf("Erro ao conectar RabbitMQ: %v", err)
	}
	defer consumer.Close()

	// Inicia goroutine do shutdown monitor
	go shutdownService.MonitorAndShutdown(ctx, cancel, counterService)

	// Inicia consumo de mensagens do RabbitMQ
	if err := consumer.ConsumeMessages(ctx); err != nil {
		log.Fatalf("Erro ao consumir mensagens: %v", err)
	}

	// Quando o contexto é cancelado pelo ShutdownService:
	// Exporta os contadores para JSON
	shutdownService.ExportJSON(counterService)

	log.Println("Serviço finalizado.")
}
