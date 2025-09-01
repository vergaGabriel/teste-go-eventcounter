package main

import (
	"context"
	"log"
	"time"

	rabbit "github.com/reb-felipe/eventcounter/infra/consumer"
	service "github.com/reb-felipe/eventcounter/infra/service"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	counterService := service.NewCounterService()

	shutdownService := &service.ShutdownService{Timeout: 5 * time.Second}

	rabbitURL := "amqp://guest:guest@localhost:5672/" // ou variável de ambiente
	consumer, err := rabbit.ConnectRabbit(rabbitURL, counterService)
	if err != nil {
		log.Fatalf("Erro ao conectar RabbitMQ: %v", err)
	}
	defer consumer.Close()

	go shutdownService.MonitorAndShutdown(ctx, cancel, counterService)

	if err := consumer.ConsumeMessages(ctx, shutdownService); err != nil {
		log.Fatalf("Erro ao consumir mensagens: %v", err)
	}

	shutdownService.ExportJSON(counterService)

	log.Println("Serviço finalizado.")
}
